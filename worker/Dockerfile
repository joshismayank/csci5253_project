FROM ubuntu:18.04
RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip3 install --upgrade pika
RUN pip3 install redis
RUN pip3 install jsonpickle
RUN pip3 install pymysql
RUN pip3 install google-cloud-logging
COPY producer-worker.py /
COPY retailer-worker.py /
COPY script.sh /
ENTRYPOINT /bin/bash script.sh
#CMD ["nohup python3 retailer-worker.py &"]
#ENTRYPOINT python3 producer-worker.py
