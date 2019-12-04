from flask import Flask, request, Response
import jsonpickle
import pika

app = Flask(__name__)


@app.route('/api/retailer/onboard', methods=['POST'])                                                                              
def retailerOnboard():
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
    #create uuid
    #save to mysql: uuid, name, location
    response = {'retailerId': uuid}
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/producer/onboard', methods=['POST'])                                                                              
def producerOnboard():
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
    #create uuid
    #save to mysql: uuid, name, location
    response = {'producerid': uuid}
    response_pickled = jsonpickle.encode(response)
    s = 200
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
    #create uuid
    #connect to rabbitmq
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.196.120.94'))
    channel = connection.channel()
    channel.exchange_declare(exchange='toWorker', exchange_type='direct')
    channel.basic_publish(exchange='toWorker', routing_key='retailer_demand', body=,properties=pika.BasicProperties(delivery_mode = 2))
    connection.close()
    #push to rabbitmq: uuid, retailerId, foodId, foodName, price, quantity
    response = {'requestId': uuid}
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
    #create uuid
    #connect to rabbitmq
    #push to rabbitmq: uuid, producerId, foodId, quantity
    response = {'requestId': uuid}
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/retailer/demand', methods=['GET'])                                                                              
def getRetailerDemand():
    retailerId = request.args.get('retailerId')
    if retailerId is None:
        response = { 'foodId' : '', 'foodName': '', 'price': '', 'quantity': '', 'timestamp': ''}
        response = [response]
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    #get from mysql: table requests - requestBy, requestUserId, isActive
    #create response
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/producer/demand', methods=['GET'])                                                                              
def getProducerDemand():
    producerId = request.args.get('producerId')
    if producerId is None:
        response = { 'foodId' : '', 'foodName': '', 'price': '', 'quantity': '', 'timestamp': ''}
        response = [response]
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    #get from mysql: table requests - requestBy, requestUserId, isActive 
    #create response
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/retailer/all', methods=['GET'])                                                                              
def getRetailers():
    #get from mysql: table retailer - isActive
    #create response
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/producer/all', methods=['GET'])                                                                              
def getProducers():
    #get from mysql: table producer - isActive
    #create response
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/retailer/demand', methods=['GET'])                                                                              
def getRetailerDemands():
    #get from mysql: table request - requestBy, isActive
    #create response
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/producer/demand', methods=['GET'])                                                                              
def getProducerDemands():
    #get from mysql: table request - requestby, isActive
    #create response
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/admin/food', methods=['POST'])                                                                              
def addFood():
    foodName = request.args.get('foodName')
    if foodName is None:
        response = {'foodId': ''}
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    #create uuid
    #store in mysql: foodId, foodName
    response = {'foodId': uuid}
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


@app.route('/api/user/food', methods=['GET'])                                                                              
def addFood():
    foodName = request.args.get('foodName')
    if foodName is None:
        response = [{'foodid' : '', 'foodName': ''}]
        s = 501
        response_pickled = jsonpickle.encode(response)
        return Response(response=response_pickled, status=s, mimetype="application/json")
    #query in mysql: foodName, isActive
    #create response
    response_pickled = jsonpickle.encode(response)
    s = 200
    return Response(response=response_pickled, status=s, mimetype="application/json")


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.247.11.169'))
    channel = connection.channel()
    channel.exchange_declare(exchange='toWorker', exchange_type='direct')
    app.run(host='0.0.0.0')
