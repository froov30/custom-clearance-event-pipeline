# Kubernetes on kind (P11) — synthetic data

**Status: applied and run on kind v0.33.0 on 2026-10-14** (results in DECISIONS "P11 results", D-061 to D-063). The steps below
are the ones that were run, with the fixes that came out of it: `enableServiceLinks: false` in every pod spec, Kafka's log on a
PVC, and loading only our own image in step 2.

Compose and kind never run together (RAM, D-039). Everything stays on D: (`KUBECONFIG` and the kind binary live under
`D:\ccp-data`; the node container and its volumes live inside Docker's disk image, which is on D:).

```powershell
. .\scripts\dev-env.ps1
docker compose --env-file .env -f deploy/compose/docker-compose.yml --profile app --profile ui down

# 1. kind v0.33.0 (10.34 MB, from the kubernetes-sigs/kind GitHub release), checksum-verified
#    https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-windows-amd64  ->  D:\ccp-data\bin\kind.exe

# 2. Cluster (node image pinned by digest in kind-config.yaml) and the app image
kind create cluster --config deploy/k8s/kind-config.yaml
kind load docker-image ccp:0.1.0 --name ccp
# Load only our image. Kafka and MongoDB are pulled by the kubelet under their pinned tags; loading them fails
# ("content digest ... not found"): their multi-platform index lists platforms Docker never pulled locally (D-062).
# kind does not see a rebuilt image. After every `docker build`, run the load line again, then:
#   kubectl -n ccp rollout restart deploy

# 3. Namespace, config, and the Secret from .env (never committed)
kubectl apply -f deploy/k8s/namespace.yaml -f deploy/k8s/configmap.yaml
$kv = Get-Content .env | Where-Object { $_ -match '^MONGO_(USER|PASSWORD)=' }
kubectl -n ccp create secret generic ccp-mongo --from-literal=$($kv[0]) --from-literal=$($kv[1])

# 4. Infrastructure, topics, services
kubectl apply -f deploy/k8s/kafka.yaml -f deploy/k8s/mongo.yaml
kubectl -n ccp rollout status statefulset/kafka statefulset/mongo --timeout=180s
kubectl apply -f deploy/k8s/topics-job.yaml
kubectl -n ccp wait --for=condition=complete job/topics-init --timeout=180s
kubectl apply -f deploy/k8s/validator.yaml -f deploy/k8s/processor.yaml -f deploy/k8s/monitor.yaml -f deploy/k8s/api.yaml
kubectl -n ccp get pods

# 5. Demo on kind: produce in-cluster, verify through a port-forward
kubectl -n ccp port-forward svc/api 8000:8000     # in a second terminal
kubectl apply -f deploy/k8s/simulator-job.yaml
kubectl -n ccp wait --for=condition=complete job/simulator --timeout=180s
python -m ccp.demo --seed 42 --verify-only

# 6. Scale-out and rebalance evidence
kubectl -n ccp scale deploy/processor --replicas=3
kubectl -n ccp logs -l app=processor --tail=50 | Select-String "partitions (assigned|revoked)"
kubectl -n ccp exec deploy/api -- python -m ccp.common.groups ccp-processor
docker stats --no-stream ccp-control-plane         # node memory (A2); there is no metrics-server for kubectl top

# 6b. Chaos check (D-063, D-067 to D-069): under load, a validator SIGKILL with 5 injected re-sends, a broker restart and
#     a MongoDB restart mid-backlog, a processor SIGKILL, scaling, then a liveness restart of a frozen processor.
#     Needs an empty `ccp` database. Each check compares every total with the manifest and every shipment with a fresh fold.
.\scripts\kind-chaos.ps1

# 7. Clean up
kind delete cluster --name ccp
```

Design notes: Kafka has a single `PLAINTEXT` listener advertised as `kafka:9092` (the Service), with the KRaft controller on
`localhost:9093`, so nothing outside the cluster can reach the broker; `ccp.common.groups` therefore runs inside a pod. Kafka
and MongoDB each keep their data on a 1 Gi PVC, so a pod restart keeps topics, committed offsets and documents. The
consumers' liveness probe is the heartbeat check (60 s, above the 14.7 s worst case measured in D-053). The simulator Job has
`backoffLimit: 0`, because a retry would produce the events twice.
