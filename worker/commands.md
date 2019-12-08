gcloud builds submit --tag gcr.io/assignments-252803/worker-server-image-1

kubectl deploy -f worker.yaml

kubectl autoscale deployment worker-server --cpu-percent=60 --min=1 --max=20
