from urllib.request import urlopen
import json
from datetime import timedelta
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging_module as log

logger = log.get('analytics')

logger.info('script started')


# define the scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('keyfile.json', scope)

# authorize the clientsheet
client = gspread.authorize(creds)

# get the instance of the Spreadsheet
# https://docs.google.com/spreadsheets/d/1hRFZoFlnQU8br1QC_cGW_r_B59bOrnHoH58Lexp5sUk/edit#gid=1667842470
sheet = client.open('out2_new3')

# get the first sheet of the Spreadsheet
sheet_instance = sheet.get_worksheet(1)
sheet = sheet_instance

# Entity codes
entities = {
    "CC": '222',
    "CN": '872',
    "CS": '1340',
    "JLC": '221',
    "Kandy": '2204',
    "MC": '1821',
    "NSBM": '2186',
    "Ruhuna": '2175',
    "SLIIT": '2188'
}
asl_office_id = 1623

# Stages of the customer funnel
stages = {
    "OP": "openings",
    "APP": "applied",
    "ACC": "an_accepted",
    "APD": "approved",
    "RE": "realized",
    "FI": "finished",
    "CO": "completed"
}

# Product funnel codes
products = {
    "GV": [7],
    "GTa": [8],
    "GTe": [9],
}

# Types of exchange
types = {
    "Incoming": "i",
    "Outgoing": "o"
}

# Read EXPA access token from file
f = open("expa_access_token.txt", "r")
access_token = f.read()


def get(start, end):
    logger.info(f"generating from {start} to {end}")

    # Send request to EXPA analytics endpoint for ASL from the given time period
    try:
        url = "https://analytics.api.aiesec.org/v2/applications/analyze.json?access_token=" + access_token + "&start_date=" + start + "&end_date=" + end + "&performance_v3%5Boffice_id%5D=" + str(asl_office_id)
        logger.info(f"sending request to {url}")
        page = urlopen(url)
        logger.info(f"response received from {url}")
        json_str = page.read()
        json_obj = json.loads(json_str)
        logger.info(f"response parsed successfully")
    except():
        logger.error("Error fetching data from EXPA analytics")
        return

    res = {}

    # for both incoming and outgoing exchanges
    for type, type_code in types.items():

        # for GV, GTa and GTe
        for product, product_codes in products.items():
            res[type_code + product] = {}

            # for each stage in the customer funnel
            for stage, stage_code in stages.items():
                res[type_code + product][stage] = {}
                res[type_code + product][stage]["ASL"] = 0

                # for each entity
                for entity, entity_code in entities.items():
                    res[type_code + product][stage][entity] = 0

                    # just in case there's multiple codes per product (GV old vs GV new)
                    for product_code in product_codes:

                        # get tag (identifier) for the current product and stage
                        # usually follows the format {type_code}}_{{stage_code}}_programme_{{product_code}}
                        # e.g. i_applied_programme_7 corresponds to applied stage in iGV
                        # the tag for OP stage is a special special case
                        if stage == "OP":
                            tag = "open" + "_" + type_code + "_" "programme" + "_" + str(product_code)
                            val = json_obj[entity_code][tag]["doc_count"]
                        else:
                            tag = type_code + "_" + stage_code + "_" + str(product_code)
                            val = json_obj[entity_code][tag]["applicants"]["value"]

                        # add value to dictionary maintaining analytics
                        res[type_code + product][stage][entity] += val

                # same as above, but for ASL
                for product_code in product_codes:

                    if stage == "OP":
                        tag = "open" + "_" + type_code + "_" "programme" + "_" + str(product_code)
                        val = json_obj[tag]["doc_count"]
                    else:
                        tag = type_code + "_" + stage_code + "_" + str(product_code)
                        val = json_obj[tag]["applicants"]["value"]

                    res[type_code + product][stage]["ASL"] += val

    # format data to required format and print to Google Sheet
    vals_x = []

    # for each product
    for product, product_dict in res.items():

        # for each stage
        for stage, stage_dict in product_dict.items():
            vals = []
            vals.append(start)
            vals.append(product)
            vals.append(stage)

            # append all values to an array
            # one row for each product, stage combination
            for entity, value in stage_dict.items():
                vals.append(str(value))

            # combine all product, stage rows into one list
            vals_x.append(vals)

    # publish to the Google Sheet
    logger.info("google sheet publishing started")
    sheet.insert_rows(vals_x, 2)
    logger.info("google sheet publishing finished")
    print(vals_x)


delta2 = timedelta(days=1)

# yesterday
start_date = datetime.now() - delta2
end_date = start_date

while start_date <= end_date:
    s = start_date.strftime("%Y-%m-%d")
    start_date += delta2
    get(s, s)

logger.info('script finished')

