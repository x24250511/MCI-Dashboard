import os

# Prometheus connection
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")

# Kubernetes namespace and container name to monitor
NAMESPACE = os.environ.get("NAMESPACE", "default")
CONTAINER = os.environ.get("CONTAINER", "flask")

# Optimal baseline values (updated after running optimal profile experiment)
OPT_THROTTLE = float(os.environ.get("OPT_THROTTLE", "0.001"))
OPT_MEM = float(os.environ.get("OPT_MEM", "0.076"))
OPT_P99 = float(os.environ.get("OPT_P99", "50.0"))
OPT_RESTART = float(os.environ.get("OPT_RESTART", "0.0"))

# Dashboard settings
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "15"))
DB_PATH = os.environ.get("DB_PATH", "mci_history.db")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "5555"))
