from elasticsearch import Elasticsearch, helpers
import csv

# Create the elasticsearch client.
es = Elasticsearch(host = "localhost", port = 9200)

# Open csv file and bulk upload
with open('/home/ben/Downloads/stockerbot-export.csv') as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='tweets')
