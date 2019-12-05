kubectl create deployment rabbitmq-server --image=marketplace.gcr.io/google/rabbitmq3:latest

kubectl expose deployment rabbitmq-server --type=LoadBalancer --port 5672 --target-port 5672
