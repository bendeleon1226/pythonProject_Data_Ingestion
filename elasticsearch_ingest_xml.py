from elasticsearch import Elasticsearch
import json
import xmltodict

# Create the elasticsearch client.
es = Elasticsearch(host = "localhost", port = 9200)

with open("resources/breakfast_menu.xml") as xml_file:
    data_dict = xmltodict.parse(xml_file.read())
    xml_file.close()

    json_data = json.dumps(data_dict)

    #print(json_data) #uncomment this line to verify json data

    es.index(index='breakfastmenu', body=json.loads(json_data))