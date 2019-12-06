import pika
import json
import pymysql
import logging
import google.cloud.logging
import pickle

connection_rabbit = pika.BlockingConnection(pika.ConnectionParameters(host='34.83.78.145'))
channel_rabbit = connection_rabbit.channel()

channel_rabbit.exchange_declare(exchange='toWorker', exchange_type='direct')

result_rabbit = channel_rabbit.queue_declare(queue='', exclusive=True)
queue_name = result_rabbit.method.queue

channel_rabbit.queue_bind(exchange='toWorker', queue=queue_name, routing_key='producer_demand')

connection = pymysql.connect(host='127.0.0.1',user='cluster_user',password='datacenter',db='dc_project')
cursor = connection.cursor()

def callback(ch, method, properties, body):
    global connection
    global cursor
    body = pickle.loads(body)
    requestId = body['requestId']
    producerId = body['producerId']
    foodId = body['foodId']
    foodName = body['foodName']
    quantity = float(body['quantity'])
    amount = 0
    query1 = """select * from acceptedRequests where foodId = %s and isActive = 1 and requestBy = %s and quantityRemaining > 0 order by price DESC"""
    query2 = """insert into acceptedRequests(foodId,foodName,quantity,price,requestBy,requestUserId,requestId) values(%s,%s,%s,%s,%s,%s,%s)"""
    query3 = """update acceptedRequests set quantityRemaining = %s where requestId = %s"""
    try:
        cursor.execute(query1,(foodId,"r"))
        results = cursor.fetchall()
        for row in results:
            curr_quantity = row[9]
            curr_price = row[3]
            curr_requestId = row[7]
            if quantity == 0:
                break
            elif quantity <= curr_quantity:
                amount = amount + quantity*curr_price
                curr_quantity = curr_quantity-quantity
                quantity = 0
                try:
                    cursor.execute(query3,(curr_quantity,curr_requestId))
                    connection.commit()
                except MySQLError as e:
                    curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
                    connection.rollback()
                    logging.warning(curr_err)
            else:
                amount = amount + curr_price*curr_quantity
                quantity = quantity - curr_quantity
                curr_quantity = 0
                try:
                    cursor.execute(query3,(curr_quantity,curr_requestId))
                    connection.commit()
                except MySQLError as e:
                    curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
                    logging.warning(curr_err)
                    connection.rollback()
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
    quantity = float(body['quantity']) - quantity
    if quantity == 0:
        price = 0
    else:
        price = amount/quantity
    try:
        cursor.execute(query2,(foodId,foodName,quantity,price,"p",producerId,requestId))
        connection.commit()
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel_rabbit.basic_consume(queue=queue_name, on_message_callback=callback)
channel_rabbit.start_consuming()
