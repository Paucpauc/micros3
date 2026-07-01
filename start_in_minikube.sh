#!/bin/bash
set -e

function kubectl() {
	minikube kubectl -- $*
}

if ! minikube status >/dev/null 2>&1; then
    echo "=== Starting Minikube ==="
    minikube start --ports=30010-30019:30010-30019
else
    echo "=== Minikube is already running ==="
fi

echo "=== Building MicroS3 Image inside Minikube ==="
minikube image build -t micros3:latest .

echo "=== Deploying MicroS3 Cluster Manifests ==="
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/service.yaml
kubectl apply -f deploy/service-leader.yaml
kubectl apply -f deploy/service-ro.yaml
kubectl apply -f deploy/statefulset.yaml

echo "=== Waiting for StatefulSet pods to be ready ==="
kubectl rollout status statefulset/micros3 --timeout=180s

echo "=== Deploy Finished Successfully ==="
echo "You can now access S3 API by port-forwarding:"
echo "  kubectl port-forward statefulset/micros3 9000:30010"
echo ""
echo "And test it with curl / aws-cli:"
echo "  curl http://localhost:9000/health"
echo "  curl http://localhost:9000/metrics"
echo ""
echo "To view cluster logs, run:"
echo "  kubectl logs -l app=micros3 -f --max-log-requests=6"
