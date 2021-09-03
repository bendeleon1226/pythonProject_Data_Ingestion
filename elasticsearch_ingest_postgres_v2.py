#!/usr/bin/python

import math
import argparse
import traceback

import psycopg2
import psycopg2.extras

import elasticsearch


def ResultIter(cursor, arraysize=100):
    # An iterator that uses fetchmany to keep memory usage down
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


def PGDatabaseConn():
    try:
        conn = psycopg2.connect(database=args.db_name,
                                user=args.db_username,
                                password=args.db_password,
                                port=args.db_port,
                                host=args.db_host)
    except Exception:
        print ("Unable to connect to postgres - host")
        traceback.print_exc()
        exit(1)
    return conn.cursor(name='mycursor',
                       cursor_factory=psycopg2.extras.RealDictCursor)


def ESConn():
    print ("Conecting to elasticsearch")
    try:
        es = elasticsearch.Elasticsearch(hosts=[args.es_host],
                                         port=args.es_port)

    except Exception:
        print ("Unable to connect to elasticsearch ")
        traceback.print_exc()
        exit(1)
    return es


def ESInsert(es, row):
    try:
        es.index(index=args.es_index, doc_type=args.es_indextype, body=row,
                 id=row.get('id'))
    except Exception as exc:
        raise exc

parser = argparse.ArgumentParser(description='ingest postgres to es')
parser.add_argument('-q', '--query',
                    required=True, help='Query to retrieve data')
parser.add_argument('--db_name',
                    required=True, help='Postgres database name')
parser.add_argument('--db_username',
                    required=True, help='Postgres database username')
parser.add_argument('--db_password',
                    required=True, help='Postgres database password')
parser.add_argument('--db_host',
                    required=True, help='Postgres host')
parser.add_argument('--db_port',
                    required=True, help='Postgres port')
parser.add_argument('--es_host',
                    required=True, help='Elastisearch host:port')
parser.add_argument('--es_port',
                    required=False, help='Elasticsearch Port')
parser.add_argument('--es_index',
                    required=True, help='Elastisearch index')
parser.add_argument('--es_indextype',
                    required=True, help='Elastisearch index type')

args = parser.parse_args()
# Conn to PG and open a cursor
cur = PGDatabaseConn()

# Execute the query
print ("Executing query")
cur.execute(args.query)

# Connect to elasticsearch
es = ESConn()

# Insert results
print ("Inserting data into elasticsearch")
error = 0
row_replace = dict()
for row in ResultIter(cursor=cur, arraysize=250):
    try:
        ESInsert(es=es, row=row)
    except Exception:
        try:
            # Try replace float nan values to None
            row_replace.clear()
            row_replace.update(row)
            for key, value in row.items():
                if isinstance(value, float) and math.isnan(value):
                    row_replace[key] = None
            print (row_replace)
            ESInsert(es, row_replace)
        except Exception:
            print ("Error inserting row")
            print (row)
            error = 1
            traceback.print_exc()
            continue

if error == 0:
    print ("Done!!! :D ")
    exit(0)
else:
    print ("Done with errors  :( ")
    exit(1)