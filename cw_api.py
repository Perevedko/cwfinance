# -*- coding: utf-8 -*-
import pika
import json
import sqlite3
import datetime
import configparser
import resources

config = configparser.ConfigParser()
config.read('config.ini')

cwuser = config['API']['USER']
cwpass = config['API']['PASS']
cwqueue = config['API']['QUEUE']

credentials = pika.PlainCredentials(cwuser, cwpass)
parameters = pika.ConnectionParameters(host='api.chtwrs.com',
                                       port=5673,
                                       virtual_host='/',
                                       credentials=credentials,
                                       ssl=True,
                                       socket_timeout=5)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

def saveRaw(store_list):
    conn = sqlite3.connect('finance.db')
    cursor = conn.cursor()
    result = cursor.execute('SELECT Count(*) FROM Resources').fetchone()
    records_number = result[0]
    records_per_hour = 60 / 5 # new record every 5 minutes
    records_per_day = records_per_hour * 24
    records_per_month = int(records_per_day * 30) # == 8640
    if records_number >= records_per_month:
        cursor.execute('DELETE FROM Resources WHERE rowid IN (SELECT rowid FROM Resources limit 1)')
    prices_for_resources = [store_list.get(name, 0) for name in resources.names_capitalized]
    cursor.execute('INSERT INTO Resources VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (datetime.datetime.now().strftime('%m-%d %H:%M'), *prices_for_resources))
    conn.commit()
    conn.close()

def parcing(recieved):
    json_array = json.loads(recieved)
    store_list = {}
    for item in json_array:
        try:
            store_list[item['name']] = item['prices'][0]
        except KeyError:
            pass
    saveRaw(store_list)

number = 0
def callback(ch, method, properties, body):
    global number
    number += 1
    parcing(body)
    print(" [x] Received %r" % number)

channel.basic_consume(callback,
                      queue=cwqueue,
                      no_ack=True)
try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()
connection.close()
