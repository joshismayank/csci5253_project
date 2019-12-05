from flask import Flask, request, Response
import jsonpickle
import pika
import uuid
import pymysql
import logging
import google.cloud.logging

app = Flask(__name__)

client = google.cloud.logging.Client()
client.setup_logging()

connection = pymysql.connect(host='127.0.0.1',user='cluster_user',password='datacenter',db='dc_project')
cursor = connection.cursor()

@app.route('/api/retailer/onboard', methods=['POST'])                                                                              
def retailerOnboard():
    global cursor
    global connection
    data = request.get_json(silent=True)
    if data is None:
        response = { 'retailerId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    name = data['name']
    location = data['location']
    if name is None or location is None:
        response = { 'retailerId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    curr_uuid =  str(uuid.uuid4())
    query = """insert into retailer(id,name,location) values(%s,%s,%s)"""
    try:
        cursor.execute(query,(curr_uuid,name,location))
        connection.commit()
        response = {'retailerId': curr_uuid}
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
        response = {'retailerId': ''}
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/producer/onboard', methods=['POST'])                                                                              
def producerOnboard():
    global cursor
    global connection
    data = request.get_json(silent=True)
    if data is None:
        response = { 'producerId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    name = data['name']
    location = data['location']
    if name is None or location is None:
        response = { 'producerId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    curr_uuid =  str(uuid.uuid4())
    query = """insert into producer(id,name,location) values(%s,%s,%s)"""
    try:
        cursor.execute(query,(curr_uuid,name,location))
        connection.commit()
        response = {'producerId': curr_uuid}
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
        response = {'producerId': ''}
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/retailer/demand', methods=['POST'])                                                                              
def retailerDemand():
    data = request.get_json(silent=True)
    if data is None:
        response = { 'requestId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    retailerId = data['retailerId']
    foodId = data['foodId']
    foodName = data['foodName']
    price = data['price']
    quantity = data['quantity']
    if retailerId is None or foodId is None or foodName is None or price is None or quantity is None:
        response = { 'requestId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    curr_uuid =  str(uuid.uuid4())
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.196.120.94'))
    channel = connection.channel()
    channel.exchange_declare(exchange='toWorker', exchange_type='direct')
    q_data = {'requestId': curr_uuid, 'retailerId': retailerId, 'foodId': fodId, 'foodName': foodName, 'price': price, 'quantity':quantity}
    channel.basic_publish(exchange='toWorker', routing_key='retailer_demand', body=q_data,properties=pika.BasicProperties(delivery_mode = 2))
    connection.close()
    response = {'requestId': curr_uuid}
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/producer/demand', methods=['POST'])                                                                              
def producerDemand():
    data = request.get_json(silent=True)
    if data is None:
        response = { 'requestId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    producerId = data['producerId']
    foodId = data['foodId']
    quantity = data['quantity']
    if producerId is None or foodId is None or quantity is None:
        response = { 'requestId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    curr_uuid =  str(uuid.uuid4())
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.196.120.94'))
    channel = connection.channel()
    channel.exchange_declare(exchange='toWorker', exchange_type='direct')
    q_data = {'requestId': curr_uuid, 'producerId': producerId, 'foodId': fodId, 'foodName': foodName, 'quantity':quantity}
    channel.basic_publish(exchange='toWorker', routing_key='producer_demand', body=q_data,properties=pika.BasicProperties(delivery_mode = 2))
    connection.close()
    response = {'requestId': curr_uuid}
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/retailer/demand', methods=['GET'])                                                                              
def getRetailerDemand():
    global cursor
    global connection
    retailerId = request.args.get('retailerId')
    if retailerId is None:
        response = { 'foodId' : '', 'foodName': '', 'price': '', 'quantity': '', 'timestamp': ''}
        response = [response]
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    query = """select * from acceptedRequests where requestBy = 'r' and isActive = 1 and requestUserId = %s"""
    try:
        cursor.execute(query,(retailerId))
        results = cursor.fetchall()
        response = []
        for row in results:
            foodId = row[0]
            foodName = row[1]
            quantity = row[2]
            price = row[3]
            createdAt = row[5]
            temp_res = {'foodId': foodId, 'foodName': foodName, 'quantity': quantity, 'price': price, 'createdAt': createdAt}
            response.append(temp_res)
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'quantity': '', 'price': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/producer/demand', methods=['GET'])                                                                              
def getProducerDemand():
    global cursor
    global connection
    producerId = request.args.get('producerId')
    if producerId is None:
        response = { 'foodId' : '', 'foodName': '', 'price': '', 'quantity': '', 'timestamp': ''}
        response = [response]
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    query = """select * from acceptedRequests where requestBy = 'p' and isActive = 1 and requestUserId = %s"""
    try:
        cursor.execute(query,(producerId))
        results = cursor.fetchall()
        response = []
        for row in results:
            foodId = row[0]
            foodName = row[1]
            quantity = row[2]
            price = row[3]
            createdAt = row[5]
            temp_res = {'foodId': foodId, 'foodName': foodName, 'quantity': quantity, 'price': price, 'createdAt': createdAt}
            response.append(temp_res)
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'quantity': '', 'price': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/retailer/all', methods=['GET'])                                                                              
def getRetailers():
    global cursor
    global connection
    query = """select * from retailer where isActive = 1"""
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        response = []
        for row in results:
            retailerId = row[0]
            name = row[1]
            location = row[2]
            temp_res = {'retailerId': retailerId, 'name': name, 'location': location}
            response.append(temp_res)
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'retailerId': '', 'name': '', 'location': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/producer/all', methods=['GET'])                                                                              
def getProducers():
    global cursor
    global connection
    query = """select * from producer where isActive = 1"""
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        response = []
        for row in results:
            producerId = row[0]
            name = row[1]
            location = row[2]
            temp_res = {'producerId': producerId, 'name': name, 'location': location}
            response.append(temp_res)
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'producerId': '', 'name': '', 'location': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/retailer/demand', methods=['GET'])                                                                              
def getRetailerDemands():
    global cursor
    global connection
    query = """select * from acceptedRequests where requestBy = 'r' and isActive = 1"""
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        response = []
        for row in results:
            foodId = row[0]
            foodName = row[1]
            quantity = row[2]
            price = row[3]
            createdAt = row[5]
            retailerId = row[8]
            temp_res = {'foodId': foodId, 'foodName': foodName, 'quantity': quantity, 'price': price, 'retailerId':retailerId, 'createdAt': createdAt}
            response.append(temp_res)
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'quantity': '', 'price': '', 'retailerId': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/producer/demand', methods=['GET'])                                                                              
def getProducerDemands():
    global cursor
    global connection
    query = """select * from acceptedRequests where requestBy = 'p' and isActive = 1"""
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        response = []
        for row in results:
            foodId = row[0]
            foodName = row[1]
            quantity = row[2]
            price = row[3]
            createdAt = row[5]
            producerId = row[8]
            temp_res = {'foodId': foodId, 'foodName': foodName, 'quantity': quantity, 'price': price, 'producerId':producerId, 'createdAt': createdAt}
            response.append(temp_res)
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'quantity': '', 'price': '', 'producerId': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/food', methods=['POST'])                                                                              
def addFood():
    global cursor
    global connection
    foodName = request.args.get('foodName')
    if foodName is None:
        response = {'foodId': ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    curr_uuid =  str(uuid.uuid4())
    query = """insert into food(id,name) values(%s,%s)"""
    try:
        cursor.execute(query,(curr_uuid,foodName))
        connection.commit()
        response = {'foodId': curr_uuid}
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
        response = {'foodId': ''}
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/user/food', methods=['GET'])                                                                              
def getFood():
    global cursor
    global connection
    foodName = request.args.get('foodName')
    if foodName is None:
        response = [{'foodid' : '', 'foodName': ''}]
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    foodName = foodName+'%'
    query = """select * from food where name like %s and isActive = 1"""
    try:
        cursor.execute(query,(foodName))
        results = cursor.fetchall()
        response = []
        for row in results:
            foodId = row[0]
            foodName = row[1]
            createdAt = row[2]
            temp_res = {'foodId': foodId, 'foodName': foodName, 'createdAt': createdAt}
            response.append(temp_res)
        s = 200
    except MySQLError as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.247.11.169'))
    channel = connection.channel()
    channel.exchange_declare(exchange='toWorker', exchange_type='direct')
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    app.run(host='0.0.0.0')
