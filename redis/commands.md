kubectl create deployment redis-server --image=marketplace.gcr.io/google/redis4:latest

kubectl expose deployment redis-server --type=LoadBalancer --port 6379 --target-port 6379
