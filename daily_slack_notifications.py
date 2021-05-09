from slack_sdk.webhook import WebhookClient
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import timedelta
from datetime import datetime
import logging_module as log

logger = log.get('daily_slack_notifications')

logger.info('script started')

entities = []
products = []

# product colours
colors = {
    "iGV": "#F85A40",
    "oGV": "#F85A40",
    "iGTa": "#0A8EA0",
    "oGTa": "#0A8EA0",
    "iGTe": "#F48924",
    "oGTe": "#F48924",
}

channels = {}
permissions = {}

# define the scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('keyfile.json', scope)

# authorize the clientsheet
client = gspread.authorize(creds)

# https://docs.google.com/spreadsheets/d/1hRFZoFlnQU8br1QC_cGW_r_B59bOrnHoH58Lexp5sUk/edit#gid=1667842470
data_sheet = client.open('out2_new3').get_worksheet(1)

# https://docs.google.com/spreadsheets/d/1dJdck5x4Pyv-XCR4QP5Ru-nMvn1lSLFzF7jXLGqa8Gk/edit#gid=0
config_sheet = client.open('Statistics Configurations').get_worksheet(0)


# send slack notifications for each entity, product combination
def send_slack_notification(entity, product, date, vals):
    # exit function if permission is not given to send notifications for the particular entity, product on a daily basis
    if not permissions[entity][product]:
        return

    url = channels[entity][product]
    webhook = WebhookClient(url)

    # Total count of all ASL entities
    if entity == "total":
        entity = "ASL"

    response = webhook.send(
        text="[{entity} {product}] EXPA update on {date}"
            .format(entity=entity, product=product, date=date),
        blocks=[
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "[{entity} {product}] EXPA update on {date}"
            .format(entity=entity, product=product, date=date)
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": " <http://stats.aiesec.lk?start={date}&end={date}&products={product}|Detailed Breakdown>"
            .format(date=date, product=product)
                }
            }
        ],
        attachments=[
            {
                "color": colors[product],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Open\t\t\t: " + vals[0]
                        }
                    }
                ]
            },
            {
                "color": colors[product],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Applied\t\t : " + vals[1]
                        }
                    }
                ]
            },
            {
                "color": colors[product],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Accepted\t  : " + vals[2]
                        }
                    }
                ]
            },
            {
                "color": colors[product],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Approved\t : " + vals[3]
                        }
                    }
                ]
            },
            {
                "color": colors[product],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Realized\t\t: " + vals[4]
                        }
                    }
                ]
            },
            {
                "color": colors[product],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Finished\t\t: " + vals[5]
                        }
                    }
                ]
            },
            {
                "color": colors[product],
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Completed\t: " + vals[6]
                        }
                    }
                ]
            },
        ]
    )

    # log.info(response.body, "Notification sent to ", entity, product, date, vals)
    logger.info(f'notification sent to {entity}, {product}, {date}, {vals}')


# get data from the Google Sheet corresponding to the given date
# https://docs.google.com/spreadsheets/d/1hRFZoFlnQU8br1QC_cGW_r_B59bOrnHoH58Lexp5sUk/edit#gid=1667842470
def get_data(date):

    logger.info(f'fetching data started for {date}')

    df = pd.DataFrame(data_sheet.get_all_records())
    df = df.loc[df['week'] == date]

    for product in products:
        df2 = df.loc[df['product'] == product]

        for entity in entities:
            vals = df2[entity].tolist()
            vals = [str(i) for i in vals]

            # send slack notification for entity, product combination
            send_slack_notification(entity, product, date, vals)

    logger.info('fetching data finished')


# get configurations from the Statistics Configurations sheet
# https://docs.google.com/spreadsheets/d/1hRFZoFlnQU8br1QC_cGW_r_B59bOrnHoH58Lexp5sUk/edit#gid=1667842470
def get_config():

    logger.info('fetching configs started')

    df = pd.DataFrame(config_sheet.get_all_records())

    # get list of entities
    global entities
    entities = df['Entity'].unique().tolist()

    # get list of products
    global products
    products = df['Product'].unique().tolist()

    # assign the webhook URLs to each entity, product combination
    global channels
    for index, row in df.iterrows():
        entity = row['Entity']
        product = row['Product']
        webhook = row['Webhook']

        if entity not in channels:
            channels[entity] = {}

        if product not in channels[entity]:
            channels[entity][product] = ""

        channels[entity][product] = webhook

    # assign permission for each entity, product combination
    global permissions
    for index, row in df.iterrows():
        entity = row['Entity']
        product = row['Product']
        permission = (row['Daily'] == 'TRUE')

        if entity not in permissions:
            permissions[entity] = {}

        if product not in permissions[entity]:
            permissions[entity][product] = False

        permissions[entity][product] = permission

    logger.info("fetching configs finished")


# get configurations
get_config()

# get data corresponding to the previous day
delta2 = timedelta(days=1)
start_date = datetime.now() - delta2
start_date = start_date.strftime("%Y-%m-%d")
get_data(start_date)

logger.info('script finished')
