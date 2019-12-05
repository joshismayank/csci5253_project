import pika
import sys
from IPython import embed
import json
import io
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from openalpr import Alpr
import hashlib
import redis
import pymysql
import logging
import google.cloud.logging

connection = pika.BlockingConnection(pika.ConnectionParameters(host=''))
channel = connection.channel()

channel.exchange_declare(exchange='toWorker', exchange_type='direct')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='toWorker', queue=queue_name, routing_key='retailer_demand')

connection = pymysql.connect(host='127.0.0.1',user='cluster_user',password='datacenter',db='dc_project')
cursor = connection.cursor()

def callback(ch, method, properties, body):
    requestId = body['requestId']
    retailerId = body['retailerId']
    foodId = body['foodId']
    foodName = body['foodName']
    price = body['price']
    quantity = body['quantity']
    query = """insert into acceptedRequests(foodId,foodName,quantity,price,requestBy,requestUserId,requestId) values(%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.execute(query,(foodId,foodName,quantity,price,"r",retailerId,requestId))
        connection.commit()
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue_name, on_message_callback=callback)
channel.start_consuming()
