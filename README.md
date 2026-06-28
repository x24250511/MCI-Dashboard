# MCI Detection Dashboard

A real-time monitoring dashboard that computes the **Composite Misconfiguration Index (MCI)** for containerised workloads running on Kubernetes. MCI is a weighted score derived from four live Prometheus signals — CPU throttle ratio (weight 0.35), memory utilisation (weight 0.25), P99 request latency (weight 0.25), and pod restart rate (weight 0.15) — each normalised against an optimal baseline and summed to produce a single number between 0 and 1. Scores below 0.05 indicate an Over-Provisioned container, 0.05–0.15 is Optimal, 0.15–0.40 is Near-Optimal, and 0.40 or above is Under-Provisioned. The dashboard provides live signal cards, historical trend charts, a Locust CSV analyser, a PromQL explorer, and actionable remediation recommendations.

## Prerequisites

- Python 3.11+ (for local/Docker deployment)
- A Kubernetes cluster with [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack) installed and Prometheus scraping your workload
- `kubectl` access to the cluster (for Kubernetes deployment)

## Quick Start — Python

```bash
# 1. Clone the repository
git clone https://github.com/tejaspatil/mci-dashboard.git
cd mci-dashboard

# 2. Copy the example environment file
cp .env.example .env

# 3. Edit .env with your Prometheus URL, namespace, and container name
nano .env

# 4. Install dependencies
pip install -r requirements.txt

# 5. Run the dashboard
python app.py
```

Open your browser at **http://localhost:5555**.

## Docker

```bash
# Build the image
docker build -t mci-dashboard .

# Run with environment variables
docker run -p 5555:5555 \
  -e PROMETHEUS_URL=http://your-prometheus:9090 \
  -e NAMESPACE=default \
  -e CONTAINER=your-container-name \
  -e OPT_THROTTLE=0.001 \
  -e OPT_MEM=0.076 \
  -e OPT_P99=50.0 \
  -e OPT_RESTART=0.0 \
  -e DASHBOARD_PORT=5555 \
  mci-dashboard
```

## Kubernetes (GKE / EKS / AKS)

```bash
# 1. Edit k8s/deploy.yaml — update PROMETHEUS_URL, NAMESPACE, and CONTAINER
nano k8s/deploy.yaml

# 2. Apply the manifests
kubectl apply -f k8s/deploy.yaml

# 3. Wait for the external IP to be assigned
kubectl get svc mci-dashboard-svc

# 4. Open your browser at EXTERNAL-IP:5555
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PROMETHEUS_URL` | `http://localhost:9090` | Base URL of your Prometheus instance |
| `NAMESPACE` | `default` | Kubernetes namespace of the monitored workload |
| `CONTAINER` | `flask` | Container name as it appears in Prometheus labels |
| `OPT_THROTTLE` | `0.001` | Optimal CPU throttle ratio baseline (fraction, e.g. 0.001 = 0.1%) |
| `OPT_MEM` | `0.076` | Optimal memory utilisation baseline (fraction, e.g. 0.076 = 7.6%) |
| `OPT_P99` | `50.0` | Optimal P99 latency baseline in milliseconds |
| `OPT_RESTART` | `0.0` | Optimal restart rate baseline (restarts/minute) |
| `REFRESH_INTERVAL` | `15` | Live monitor polling interval in seconds |
| `DASHBOARD_PORT` | `5555` | Port the Flask app listens on |

## License

MIT
