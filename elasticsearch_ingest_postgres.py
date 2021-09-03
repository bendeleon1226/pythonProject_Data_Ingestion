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
        conn = psycopg2.connect(user="postgres",
                                  password="Test1236",
                                  host="localhost",
                                  port="5432",
                                  database="postgres")
    except Exception:
        print ("Unable to connect to postgres - host")
        traceback.print_exc()
        exit(1)
    return conn.cursor(name='mycursor',
                       cursor_factory=psycopg2.extras.RealDictCursor)


def ESConn():
    print ("Conecting to elasticsearch")
    try:
        es = elasticsearch.Elasticsearch(hhost = "localhost",
                                         port = 9200)

    except Exception:
        print ("Unable to connect to elasticsearch ")
        traceback.print_exc()
        exit(1)
    return es


def ESInsert(es, row):
    try:
        es.index(index='users', doc_type='table_users', body=row,
                 id=row.get('id'))
    except Exception as exc:
        raise exc

# Conn to PG and open a cursor
cur = PGDatabaseConn()

# Execute the query
print ("Executing query")
cur.execute("select * from users")

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