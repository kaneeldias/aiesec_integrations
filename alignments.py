from urllib.request import urlopen
import json

url = "https://gis-api.aiesec.org/v2/lists/mcs_alignments.json?mc_id[]=495&amp;mc_id[]=537"
page = urlopen(url)
json_str = page.read()
json_obj = json.loads(json_str)



for entity in json_obj:
    if (entity['id'] != 1623):
        continue

    f = open("alignments.txt", "w", encoding='utf-8')

    alignments = entity['alignments']
    for alignment in alignments:
        data_id = alignment['alignment_id']
        name = alignment['value']
        value = alignment['id']
        line = f'<option data-id="{data_id}" value="{value}">{name}</option>\n'
        print(line)
        f.write(line)

    f.close()

