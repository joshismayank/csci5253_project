gcloud builds submit --tag gcr.io/assignments-252803/rest-server-image-1

gcloud container clusters create project-cluster --zone us-west1-a --num-nodes 2 --enable-autoscaling --min-nodes 1 --max-nodes 4

kubectl create secret generic cloudsql-instance-credentials --from-file=credentials.json=assignments-252803-33a1b2c6e0bd.json

kubectl create secret generic cloudsql-db-credentials --from-literal=username=cluster_user --from-literal=password=datacenter

kubectl apply -f rest-server.yaml

kubectl expose deployment rest-server --type=LoadBalancer --port 5000 --target-port 5000

kubectl autoscale deployment rest-server --cpu-percent=70 --min=1 --max=20
