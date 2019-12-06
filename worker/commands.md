gcloud builds submit --tag gcr.io/assignments-252803/worker-server-image-1

kubectl deploy -f worker.yaml
