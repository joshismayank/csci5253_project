from flask import Flask, request, Response
import jsonpickle
import pika
import uuid
import pymysql
import logging
import google.cloud.logging
import redis
import pickle

app = Flask(__name__)

client = google.cloud.logging.Client()
client.setup_logging()

def getConnection():
    connection = pymysql.connect(host='127.0.0.1',user='cluster_user',password='datacenter',db='dc_project')
    return connection


@app.route('/api/retailer/onboard', methods=['POST'])                                                                              
def retailerOnboard():
    connection = getConnection()
    cursor = connection.cursor()
    data = request.get_json(silent=True)
    if data is None:
        response = { 'retailerId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    name = None
    location = None
    if 'name' in data:
        name = data['name']
    if 'location' in data:
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
        connection.close()
        response = {'retailerId': curr_uuid}
        s = 200
    except Exception as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
        connection.close()
        response = {'retailerId': ''}
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/producer/onboard', methods=['POST'])                                                                              
def producerOnboard():
    connection = getConnection()
    cursor = connection.cursor()
    data = request.get_json(silent=True)
    if data is None:
        response = { 'producerId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    name = None
    location = None
    if 'name' in data:
        name = data['name']
    if 'location' in data:
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
        connection.close()
        response = {'producerId': curr_uuid}
        s = 200
    except Exception as e:
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
        connection.close()
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
    retailerId = None
    foodId = None
    foodName = None
    price = None
    quantity = None
    if 'retailerId' in data:
        retailerId = data['retailerId']
    if 'foodId' in data:
        foodId = data['foodId']
    if 'foodName' in data:
        foodName = data['foodName']
    if 'price' in data:
        price = data['price']
    if 'quantity' in data:
        quantity = data['quantity']
    if retailerId is None or foodId is None or foodName is None or price is None or quantity is None:
        response = { 'requestId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    curr_uuid =  str(uuid.uuid4())
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.247.11.169'))
    channel = connection.channel()
    channel.exchange_declare(exchange='toWorker', exchange_type='direct')
    q_data = pickle.dumps({'requestId': curr_uuid, 'retailerId': retailerId, 'foodId': foodId, 'foodName': foodName, 'price': price, 'quantity':quantity})
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
    producerId = None
    foodId = None
    foodName = None
    quantity = None
    if 'producerId' in data:
        producerId = data['producerId']
    if 'foodId' in data:
        foodId = data['foodId']
    if 'foodName' in data:
        foodName = data['foodName']
    if 'quantity' in data:
        quantity = data['quantity']
    if producerId is None or foodId is None or quantity is None or foodName is None:
        response = { 'requestId' : ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    curr_uuid =  str(uuid.uuid4())
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.247.11.169'))
    channel = connection.channel()
    channel.exchange_declare(exchange='toWorker', exchange_type='direct')
    q_data = pickle.dumps({'requestId': curr_uuid, 'producerId': producerId, 'foodId': foodId, 'foodName': foodName, 'quantity':quantity})
    channel.basic_publish(exchange='toWorker', routing_key='producer_demand', body=q_data,properties=pika.BasicProperties(delivery_mode = 2))
    connection.close()
    response = {'requestId': curr_uuid}
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/retailer/demand', methods=['GET'])                                                                              
def getRetailerDemand():
    connection = getConnection()
    cursor = connection.cursor()
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
        connection.close()
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
    except Exception as e:
        connection.close()
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'quantity': '', 'price': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/producer/demand', methods=['GET'])                                                                              
def getProducerDemand():
    connection = getConnection()
    cursor = connection.cursor()
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
        connection.close()
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
    except Exception as e:
        connection.close()
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'quantity': '', 'price': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/retailer/all', methods=['GET'])                                                                              
def getRetailers():
    connection = getConnection()
    cursor = connection.cursor()
    query = """select * from retailer where isActive = 1"""
    try:
        cursor.execute(query)
        connection.close()
        results = cursor.fetchall()
        response = []
        for row in results:
            retailerId = row[0]
            name = row[1]
            location = row[2]
            temp_res = {'retailerId': retailerId, 'name': name, 'location': location}
            response.append(temp_res)
        s = 200
    except Exception as e:
        connection.close()
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'retailerId': '', 'name': '', 'location': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/producer/all', methods=['GET'])                                                                              
def getProducers():
    connection = getConnection()
    cursor = connection.cursor()
    query = """select * from producer where isActive = 1"""
    try:
        cursor.execute(query)
        connection.close()
        results = cursor.fetchall()
        response = []
        for row in results:
            producerId = row[0]
            name = row[1]
            location = row[2]
            temp_res = {'producerId': producerId, 'name': name, 'location': location}
            response.append(temp_res)
        s = 200
    except Exception as e:
        connection.close()
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'producerId': '', 'name': '', 'location': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/retailer/demand', methods=['GET'])                                                                              
def getRetailerDemands():
    connection = getConnection()
    cursor = connection.cursor()
    query = """select * from acceptedRequests where requestBy = 'r' and isActive = 1 and quantityRemaining > 0"""
    try:
        cursor.execute(query)
        connection.close()
        results = cursor.fetchall()
        response = []
        for row in results:
            foodId = row[0]
            foodName = row[1]
            quantity = row[2]
            price = row[3]
            createdAt = row[5]
            retailerId = row[8]
            quantityRemaining = row[9]
            temp_res = {'foodId': foodId, 'foodName': foodName, 'quantity': quantity, 'quantityRemaining': quantityRemaining, 'price': price, 'retailerId':retailerId, 'createdAt': createdAt}
            response.append(temp_res)
        s = 200
    except Exception as e:
        connection.close()
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'quantity': '', 'price': '', 'retailerId': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/producer/demand', methods=['GET'])                                                                              
def getProducerDemands():
    connection = getConnection()
    cursor = connection.cursor()
    query = """select * from acceptedRequests where requestBy = 'p' and isActive = 1"""
    try:
        cursor.execute(query)
        connection.close()
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
    except Exception as e:
        connection.close()
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'quantity': '', 'price': '', 'producerId': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/food', methods=['POST'])                                                                              
def addFood():
    connection = getConnection()
    cursor = connection.cursor()
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
        connection.close()
        connection.commit()
        response = {'foodId': curr_uuid}
        s = 200
    except Exception as e:
        connection.close()
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        connection.rollback()
        response = {'foodId': ''}
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/user/food/all', methods=['GET'])                                                                              
def getAllFood():
    connection = getConnection()
    cursor = connection.cursor()
    query = """select * from food where isActive = 1"""
    try:
        cursor.execute(query)
        connection.close()
        results = cursor.fetchall()
        response = []
        for row in results:
            foodId = row[0]
            foodName = row[1]
            createdAt = row[2]
            temp_res = {'foodId': foodId, 'foodName': foodName, 'createdAt': createdAt}
            response.append(temp_res)
        s = 200
    except Exception as e:
        connection.close()
        curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
        logging.warning(curr_err)
        response = [{'foodId': '', 'foodName': '', 'createdAt': ''}]
        s = 501
    response_pickled = jsonpickle.encode(response)
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/user/food', methods=['GET'])                                                                              
def getFood():
    connection = getConnection()
    cursor = connection.cursor()
    foodName = request.args.get('foodName')
    foodId = request.args.get('foodId')
    if foodName is None and foodId is None:
        response = {'foodId' : '', 'foodName': ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    query = None
    word = None
    if foodId is None:
        query = """select * from food where name = %s and isActive = 1"""
        word = foodName
    else:
        query = """select * from food where id = %s and isActive = 1"""
        word = foodId
    r = redis.Redis(host='35.203.139.73', port=6379, db=1)
    if r.exists(word):
        temp = r.get(word)
        temp = temp.decode("utf-8","replace")
        foodId = temp.split(":")[0]
        foodName = temp.split(":")[1]
        response = {'foodId': foodId, 'foodName': foodName}
        s = 200
    else:
        try:
            cursor.execute(query,(word))
            connection.close()
            results = cursor.fetchall()
            logging.warning("got results")
            if not results:
                response = {'foodId': '', 'foodName': ''}
                s = 200
            else:
                foodId = results[0][0]
                foodName = results[0][1]
                createdAt = results[0][2]
                response = {'foodId': foodId, 'foodName': foodName}
                s = 200
                temp = foodId + ":" + foodName
                temp = temp.encode("utf-8")
                r.set(word,temp)
                if r.exists("total"):
                    tot = r.get("total")+1
                    r.set("total",tot)
                    if tot > 200:
                        r.flushdb()
                else:
                    r.set("total",1)
        except Exception as e:
            connection.close()
            curr_err = 'Got error {!r}, errno is {}'.format(e, e.args[0])
            logging.warning(curr_err)
            response = {'foodId': '', 'foodName': ''}
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
