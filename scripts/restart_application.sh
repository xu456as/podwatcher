kubectl delete sparkapplication spark-pi-yunikorn -n default
sleep 2
kubectl apply -f setup/spark-pi.yaml
