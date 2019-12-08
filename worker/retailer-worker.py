import pika
import pymysql
import logging
import google.cloud.logging
import pickle

connection_rabbit = pika.BlockingConnection(pika.ConnectionParameters(host='35.247.11.169'))
channel_rabbit = connection_rabbit.channel()

channel_rabbit.exchange_declare(exchange='toWorker', exchange_type='direct')

result_rabbit = channel_rabbit.queue_declare(queue='', exclusive=True)
queue_name = result_rabbit.method.queue

channel_rabbit.queue_bind(exchange='toWorker', queue=queue_name, routing_key='retailer_demand')

connection = pymysql.connect(host='127.0.0.1',user='cluster_user',password='datacenter',db='dc_project')
cursor = connection.cursor()

def callback(ch, method, properties, body):
    logging.warning("reached callback - retailer")
    body = pickle.loads(body)
    global connection
    global cursor
    requestId = body['requestId']
    retailerId = body['retailerId']
    foodId = body['foodId']
    foodName = body['foodName']
    price = body['price']
    quantity = body['quantity']
    query = """insert into acceptedRequests(foodId,foodName,quantity,price,requestBy,requestUserId,requestId,quantityRemaining) values(%s,%s,%s,%s,%s,%s,%s,%s)"""
    try:
        cursor.execute(query,(foodId,foodName,quantity,price,"r",retailerId,requestId,quantity))
        connection.commit()
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel_rabbit.basic_consume(queue=queue_name, on_message_callback=callback)
channel_rabbit.start_consuming()
