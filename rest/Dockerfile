FROM ubuntu:18.04
RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip3 install --upgrade pika
RUN pip3 install redis
RUN pip3 install jsonpickle
RUN pip3 install Pillow
RUN pip3 install pymysql
RUN pip3 install google-cloud-logging
RUN git clone https://github.com/pallets/flask
WORKDIR /flask/examples/tutorial
RUN python3 setup.py install && pip3 install -e .
WORKDIR /
COPY rest-server.py /
ENTRYPOINT export LC_ALL=C.UTF-8 && export LANG=C.UTF-8 && export FLASK_APP=rest-server.py && flask run -h 0.0.0.0
