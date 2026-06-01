# Charmed Airflow + Spark Demo

## What This Does

Deploys a full Airflow + Spark pipeline on Kubernetes via Juju that demonstrates:
- **KubernetesExecutor** spawning **5 worker pods** (3 running simultaneously)
- **KubernetesPodOperator** creating **Spark pods** in a separate namespace
- **Fan-out/fan-in** pattern: 1 ingest → 3 parallel analytics → 1 summary
- Real-time pod creation visible via `kubectl`

## Quick Start

```bash
cd ~/Desktop/airflow-spark-demo

# Deploy everything (takes ~10 minutes for all charms to settle)
just deploy

# Wait for all units to be active
just wait-ready

# Get the Airflow UI address
just get-ui-ip

# Trigger the DAG
just trigger

# WATCH PODS SPAWNING IN REAL-TIME (the demo money shot)
just watch          # worker pods in airflow-spark namespace
just watch-spark    # spark pods in spark-workloads namespace

# Check task progress
just status

# View output
just logs-all
```

## Demo Flow

```
                    +---> analyze_categories (Spark) --+
                    |                                   |
ingest_data --------+---> analyze_regions   (Spark) ---+---> executive_summary
                    |                                   |
                    +---> analyze_trends    (Spark) --+
```

During the parallel phase, you'll see:
- 3 worker pods in `airflow-spark` namespace (created by KubernetesExecutor)
- 3 spark pods in `spark-workloads` namespace (created by KubernetesPodOperator)
- Total of **6 pods running simultaneously**

## Validation Commands

```bash
# Live pod watch (run BEFORE triggering)
watch kubectl get pods -n airflow-spark -n spark-workloads

# Or in two terminals:
kubectl get pods -n airflow-spark -w      # Terminal 1
kubectl get pods -n spark-workloads -w    # Terminal 2

# Check task states
just status

# View MinIO bucket contents (data written by Spark)
kubectl exec deploy/minio -n airflow-spark -- mc ls local/airflow-spark/demo/
kubectl exec deploy/minio -n airflow-spark -- mc ls local/airflow-spark/demo/results/

# Scheduler logs (see KubernetesExecutor creating pods)
kubectl logs airflow-scheduler-k8s-0 -c airflow-scheduler -n airflow-spark --since=60s | grep -i "creating\|pod\|parallel"

# Clean up between runs
just clean-pods
just trigger
```

## Teardown

```bash
just teardown
```
