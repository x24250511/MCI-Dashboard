import math
import time
import threading
import statistics
import requests
import sqlite3
import csv
import io
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from config import (
    PROMETHEUS_URL, NAMESPACE, CONTAINER,
    OPT_THROTTLE, OPT_MEM, OPT_P99, OPT_RESTART,
    DB_PATH, DASHBOARD_PORT
)

app = Flask(__name__)

# Runtime-mutable Prometheus URL — starts from config but overridable via API
_active_prometheus_url = PROMETHEUS_URL

# ── Latency probe state ───────────────────────────────────────────────────────
_probe_lock = threading.Lock()

# ── Database ──────────────────────────────────────────────────────────────────


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS mci_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       TEXT,
        pod             TEXT,
        throttle_ratio  REAL,
        mem_util        REAL,
        latency_p99     REAL,
        restart_rate    REAL,
        mci_score       REAL,
        classification  TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT
    )''')
    conn.commit()
    conn.close()


def get_setting(key, default=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key=?', (key,))
    row = c.fetchone()
    conn.close()
    return float(row[0]) if row else default


def set_setting(key, value):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)',
              (key, str(value)))
    conn.commit()
    conn.close()


def log_to_db(pod, throttle, mem, p99, restart, mci, classification):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT INTO mci_log
        (timestamp, pod, throttle_ratio, mem_util,
         latency_p99, restart_rate, mci_score, classification)
        VALUES (?,?,?,?,?,?,?,?)''',
              (datetime.now().isoformat(), pod,
               throttle, mem, p99, restart, mci, classification))
    conn.commit()
    conn.close()


def get_history(limit=500):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''SELECT timestamp, pod, throttle_ratio, mem_util,
                 latency_p99, restart_rate, mci_score, classification
                 FROM mci_log ORDER BY id DESC LIMIT ?''', (limit,))
    rows = c.fetchall()
    conn.close()
    return rows

# ── Prometheus ────────────────────────────────────────────────────────────────


def query_prometheus(promql):
    try:
        r = requests.get(
            f"{_active_prometheus_url}/api/v1/query",
            params={"query": promql},
            timeout=5
        )
        data = r.json()
        if data["status"] == "success" and data["data"]["result"]:
            v = float(data["data"]["result"][0]["value"][1])
            # Prometheus returns "NaN"/"Inf" for degenerate queries (e.g. 0/0)
            return 0.0 if not math.isfinite(v) else v
        return 0.0
    except Exception:
        return 0.0


def get_live_signals():
    # ThrottleRatio: split into two sum-queries and divide in Python so that a
    # zero-denominator (fresh container or counter reset after restart) never
    # produces a PromQL NaN that leaks into the dashboard.
    throttle_throttled = query_prometheus(
        f'sum(rate(container_cpu_cfs_throttled_periods_total'
        f'{{namespace="{NAMESPACE}",container="{CONTAINER}"}}[2m]))'
    )
    throttle_total = query_prometheus(
        f'sum(rate(container_cpu_cfs_periods_total'
        f'{{namespace="{NAMESPACE}",container="{CONTAINER}"}}[2m]))'
    )
    throttle = throttle_throttled / throttle_total if throttle_total > 0 else 0.0

    # MemUtil: sum() collapses multi-replica series so result[0] is always the
    # correct aggregate, not a random pod's value.
    mem_bytes = query_prometheus(
        f'sum(container_memory_working_set_bytes'
        f'{{namespace="{NAMESPACE}",container="{CONTAINER}"}})'
    )
    mem_limit = query_prometheus(
        f'sum(kube_pod_container_resource_limits'
        f'{{namespace="{NAMESPACE}",container="{CONTAINER}",'
        f'resource="memory",unit="byte"}})'
    )
    mem = mem_bytes / mem_limit if mem_limit > 0 else 0.0

    # RestartRate: sum() for multi-replica consistency; [5m] window gives
    # faster OOM-kill detection than the previous [10m].
    restart = query_prometheus(
        f'sum(rate(kube_pod_container_status_restarts_total'
        f'{{namespace="{NAMESPACE}",container="{CONTAINER}"}}[5m]))'
    )

    return throttle, mem, restart

# ── Latency probe ─────────────────────────────────────────────────────────────


def probe_latency(target_url, samples=20):
    """Hit /cpu endpoint N times and calculate P99 latency automatically."""
    times = []
    try:
        for _ in range(samples):
            start = time.time()
            r = requests.get(f"{target_url}/cpu", timeout=5)
            elapsed = (time.time() - start) * 1000
            if r.status_code == 200:
                times.append(elapsed)
    except Exception:
        pass

    if len(times) >= 5:
        times.sort()
        idx = max(0, int(len(times) * 0.99) - 1)
        p99 = round(times[idx], 2)
        with _probe_lock:
            set_setting("last_p99", p99)
            set_setting("p99_source", "probe")


def get_current_p99():
    return get_setting("last_p99", OPT_P99)


def get_p99_source():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key="p99_source"')
    row = c.fetchone()
    conn.close()
    return row[0] if row else "baseline"

# ── MCI Calculation ───────────────────────────────────────────────────────────


def calculate_mci(throttle, mem, p99, restart):
    def norm(x, xopt):
        return max(0.0, (x - xopt) / xopt) if xopt > 0 else 0.0

    N_t = norm(throttle, OPT_THROTTLE)
    N_m = norm(mem,      OPT_MEM)
    N_l = norm(p99,      OPT_P99)
    N_r = norm(restart,  OPT_RESTART)

    mci = round(0.35*N_t + 0.25*N_m + 0.25*N_l + 0.15*N_r, 4)
    return mci, round(N_t, 4), round(N_m, 4), round(N_l, 4), round(N_r, 4)


def classify(mci, throttle=None, mem=None):
    if mci >= 0.40:
        return "Under-Provisioned", "#e74c3c"
    elif mci >= 0.15:
        return "Near-Optimal",      "#f59e0b"
    elif mci >= 0.05:
        return "Optimal",           "#27ae60"
    else:
        # MCI < 0.05: only call it Over-Provisioned when resources are genuinely
        # wasted — memory < 5 % of limit AND zero CPU throttling.  If the
        # container is actually using resources (mem > 5 % or any throttling),
        # it is running Optimally at rest, not over-provisioned.
        if throttle is not None and mem is not None:
            if mem < 0.05 and throttle == 0.0:
                return "Over-Provisioned", "#3498db"
            return "Optimal", "#27ae60"
        return "Over-Provisioned",  "#3498db"


def build_response(pod, throttle, mem, p99, restart, source="live"):
    mci, N_t, N_m, N_l, N_r = calculate_mci(throttle, mem, p99, restart)
    label, color = classify(mci, throttle, mem)
    log_to_db(pod, throttle, mem, p99, restart, mci, label)
    return {
        "mci":            mci,
        "classification": label,
        "color":          color,
        "source":         source,
        "signals": {
            "throttle_ratio": round(throttle, 4),
            "mem_util":       round(mem,      4),
            "latency_p99":    round(p99,      2),
            "restart_rate":   round(restart,  6)
        },
        "normalised": {
            "N_throttle": N_t,
            "N_mem":      N_m,
            "N_latency":  N_l,
            "N_restart":  N_r
        },
        "baseline": {
            "OPT_THROTTLE": OPT_THROTTLE,
            "OPT_MEM":      OPT_MEM,
            "OPT_P99":      OPT_P99,
            "OPT_RESTART":  OPT_RESTART
        },
        "p99_source": get_p99_source(),
        "timestamp":  datetime.now().isoformat()
    }

# ── Recommendations ───────────────────────────────────────────────────────────


def generate_recommendations(signals, normalised, classification, mci):
    recommendations = []
    s = signals

    # CPU Throttling
    if s["throttle_ratio"] > 0.30:
        recommendations.append({
            "severity":        "critical",
            "signal":          "CPU Throttling",
            "observation":     f"ThrottleRatio is {s['throttle_ratio']*100:.1f}% — container throttled in 1 of every {1/s['throttle_ratio']:.0f} scheduling windows",
            "cause":           "CPU limit is severely below workload demand — CFS bandwidth control is suspending the container repeatedly",
            "action":          "Increase CPU limit immediately",
            "suggested_change": "Double your current CPU limit as a starting point, then re-run the workload test",
            "expected_outcome": "ThrottleRatio should drop below 10% and P99 latency should improve significantly"
        })
    elif s["throttle_ratio"] > 0.10:
        recommendations.append({
            "severity":        "warning",
            "signal":          "CPU Throttling",
            "observation":     f"ThrottleRatio is {s['throttle_ratio']*100:.1f}% — above the 10% acceptable threshold",
            "cause":           "CPU limit is slightly below workload demand, especially under burst conditions",
            "action":          "Increase CPU limit by 50%",
            "suggested_change": "Increase CPU limit by 50% and monitor under burst load pattern",
            "expected_outcome": "ThrottleRatio should drop below 10%"
        })
    elif s["throttle_ratio"] < 0.01 and mci < 0.05:
        recommendations.append({
            "severity":        "info",
            "signal":          "CPU Throttling",
            "observation":     f"ThrottleRatio is {s['throttle_ratio']*100:.1f}% — well below threshold",
            "cause":           "CPU limit may be higher than the workload requires",
            "action":          "Consider reducing CPU limit to reduce cloud cost",
            "suggested_change": "Reduce CPU limit by 25% increments and monitor for throttling after each step",
            "expected_outcome": "Cost reduction with no performance impact"
        })

    # Memory
    if s["mem_util"] > 0.85:
        recommendations.append({
            "severity":        "critical",
            "signal":          "Memory Utilisation",
            "observation":     f"MemUtil is {s['mem_util']*100:.1f}% — OOM kill is imminent",
            "cause":           "Memory limit is too low — the kernel will terminate the container",
            "action":          "Increase memory limit immediately",
            "suggested_change": "Increase memory limit by at least 50% immediately to stop crash loop",
            "expected_outcome": "Pod restarts will stop and memory pressure will ease"
        })
    elif s["mem_util"] > 0.70:
        recommendations.append({
            "severity":        "warning",
            "signal":          "Memory Utilisation",
            "observation":     f"MemUtil is {s['mem_util']*100:.1f}% — approaching the 85% danger zone",
            "cause":           "Memory headroom is insufficient for traffic spikes",
            "action":          "Increase memory limit by 30%",
            "suggested_change": "Increase memory limit by 30% as a precaution before running burst tests",
            "expected_outcome": "Safe headroom against unexpected memory spikes"
        })
    elif s["mem_util"] < 0.20 and mci < 0.05:
        recommendations.append({
            "severity":        "info",
            "signal":          "Memory Utilisation",
            "observation":     f"MemUtil is {s['mem_util']*100:.1f}% — memory is under-utilised",
            "cause":           "Memory limit may be higher than needed",
            "action":          "Consider reducing memory limit to save cost",
            "suggested_change": "Reduce memory limit by 25% and monitor for OOM events",
            "expected_outcome": "Cost reduction with no stability impact"
        })

    # Latency
    if s["latency_p99"] > 500:
        recommendations.append({
            "severity":        "critical",
            "signal":          "P99 Latency",
            "observation":     f"P99 latency is {s['latency_p99']:.0f}ms — SLA breach threshold exceeded",
            "cause":           "High tail latency is most likely caused by CPU throttling stalling request processing",
            "action":          "Resolve CPU throttling first — latency is a downstream symptom",
            "suggested_change": "Fix CPU limit first. If throttling resolves but latency remains high, investigate application-level bottlenecks",
            "expected_outcome": "P99 should drop below 200ms once throttling is resolved"
        })
    elif s["latency_p99"] > 200:
        recommendations.append({
            "severity":        "warning",
            "signal":          "P99 Latency",
            "observation":     f"P99 latency is {s['latency_p99']:.0f}ms — elevated but below SLA breach",
            "cause":           "Moderate resource contention is affecting tail latency",
            "action":          "Monitor under burst load to verify SLA is maintained",
            "suggested_change": "Run burst workload test and confirm P99 stays below 500ms under peak traffic",
            "expected_outcome": "Confirm configuration holds under peak traffic conditions"
        })

    # Restart rate
    if s["restart_rate"] > 0:
        recommendations.append({
            "severity":        "critical",
            "signal":          "Pod Restart Rate",
            "observation":     f"RestartRate is {s['restart_rate']:.4f}/min — OOM kills are occurring",
            "cause":           "Container is being killed by the kernel due to memory limit breach",
            "action":          "Increase memory limit immediately",
            "suggested_change": "Double memory limit immediately to stop the crash loop",
            "expected_outcome": "Restart rate drops to zero once memory limit is sufficient"
        })

    # Overall
    if classification == "Optimal":
        recommendations.append({
            "severity":        "success",
            "signal":          "Overall Assessment",
            "observation":     f"MCI score {mci:.4f} — configuration is well tuned",
            "cause":           "All four signals are within acceptable thresholds",
            "action":          "No changes needed at this time",
            "suggested_change": "Run burst and progressive workload tests to confirm stability under varying load",
            "expected_outcome": "MCI should remain below 0.15 across all workload patterns"
        })
    elif classification == "Over-Provisioned":
        recommendations.append({
            "severity":        "info",
            "signal":          "Overall Assessment",
            "observation":     f"MCI score {mci:.4f} — resources are over-allocated",
            "cause":           "CPU and memory limits are higher than the workload requires",
            "action":          "Reduce resource limits incrementally to optimise cost",
            "suggested_change": "Reduce CPU limit by 25% increments until ThrottleRatio reaches 5-8%, then stop",
            "expected_outcome": "Cost reduction with MCI remaining in the Optimal range"
        })
    elif classification == "Near-Optimal":
        recommendations.append({
            "severity":        "warning",
            "signal":          "Overall Assessment",
            "observation":     f"MCI score {mci:.4f} — configuration is slightly under-tuned",
            "cause":           "One or more signals are mildly elevated above optimal thresholds",
            "action":          "Fine-tune resource limits — small incremental increases",
            "suggested_change": "Increase the dominant signal resource by 20-30% and re-test",
            "expected_outcome": "MCI should drop into the Optimal range (0.05 - 0.15)"
        })

    return recommendations

# ── Routes ────────────────────────────────────────────────────────────────────


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/live")
def api_live():
    throttle, mem, restart = get_live_signals()
    p99 = get_current_p99()
    return jsonify(build_response(
        "flask-default", throttle, mem, p99, restart, "live"))


@app.route("/api/history")
def api_history():
    rows = get_history()
    return jsonify([{
        "timestamp":      r[0],
        "pod":            r[1],
        "throttle_ratio": r[2],
        "mem_util":       r[3],
        "latency_p99":    r[4],
        "restart_rate":   r[5],
        "mci_score":      r[6],
        "classification": r[7]
    } for r in rows])


@app.route("/api/upload_csv", methods=["POST"])
def upload_csv():
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file uploaded"}), 400

    content = file.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    results = []
    throttle, mem, restart = get_live_signals()

    for row in reader:
        name = row.get("Name", "").strip()
        if name not in ("Aggregated", "/cpu"):
            continue
        try:
            p99 = float(row.get("99%") or row.get(
                "99th Percentile") or OPT_P99)
            avg = float(row.get("Average Response Time", 0))
            req_count = int(row.get("Request Count", 0))
            fail_count = int(row.get("Failure Count", 0))

            if name == "Aggregated":
                set_setting("last_p99", p99)
                set_setting("p99_source", "csv")

            result = build_response(
                f"csv-{name}", throttle, mem, p99, restart, "csv")
            result["csv_stats"] = {
                "name":        name,
                "p99":         p99,
                "avg":         round(avg, 2),
                "req_count":   req_count,
                "fail_count":  fail_count,
                "failure_pct": round(fail_count/req_count*100, 1) if req_count else 0
            }
            results.append(result)
        except Exception:
            continue

    return jsonify(results)


@app.route("/api/probe_latency", methods=["POST"])
def api_probe_latency():
    data = request.get_json() or {}
    target = data.get("target_url", "http://localhost:5002")
    t = threading.Thread(
        target=probe_latency,
        args=(target, 20),
        daemon=True
    )
    t.start()
    return jsonify({
        "status":  "probing",
        "target":  target,
        "samples": 20,
        "message": "Probing 20 requests — refresh in 10 seconds"
    })


@app.route("/api/debug/history")
def api_debug_history():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM mci_log')
    total = c.fetchone()[0]
    c.execute('''SELECT id, timestamp, mci_score, throttle_ratio, mem_util, classification
                 FROM mci_log ORDER BY id DESC LIMIT 5''')
    last5 = [{"id": r[0], "timestamp": r[1], "mci_score": r[2],
               "throttle_ratio": r[3], "mem_util": r[4], "classification": r[5]}
             for r in c.fetchall()]
    conn.close()
    return jsonify({"total_rows": total, "last_5": last5})


@app.route("/api/probe_status")
def api_probe_status():
    return jsonify({
        "p99":    get_current_p99(),
        "source": get_p99_source()
    })


@app.route("/api/recommendations")
def api_recommendations():
    throttle, mem, restart = get_live_signals()
    p99 = get_current_p99()
    mci, N_t, N_m, N_l, N_r = calculate_mci(throttle, mem, p99, restart)
    label, color = classify(mci, throttle, mem)
    signals = {
        "throttle_ratio": round(throttle, 4),
        "mem_util":       round(mem,      4),
        "latency_p99":    p99,
        "restart_rate":   round(restart,  6)
    }
    normalised = {
        "N_throttle": N_t,
        "N_mem":      N_m,
        "N_latency":  N_l,
        "N_restart":  N_r
    }
    recs = generate_recommendations(signals, normalised, label, mci)
    return jsonify({
        "mci":             mci,
        "classification":  label,
        "color":           color,
        "recommendations": recs
    })


@app.route("/api/config")
def api_config():
    return jsonify({
        "prometheus_url": _active_prometheus_url,
        "namespace":      NAMESPACE,
        "container":      CONTAINER,
        "baseline": {
            "OPT_THROTTLE": OPT_THROTTLE,
            "OPT_MEM":      OPT_MEM,
            "OPT_P99":      OPT_P99,
            "OPT_RESTART":  OPT_RESTART
        }
    })


@app.route("/api/set_prometheus_url", methods=["POST"])
def api_set_prometheus_url():
    global _active_prometheus_url
    data = request.get_json() or {}
    url = data.get("url", "").strip().rstrip("/")
    if not url:
        return jsonify({"status": "error", "message": "URL is required"}), 400
    try:
        r = requests.get(
            f"{url}/api/v1/query",
            params={"query": "up"},
            timeout=3
        )
        resp = r.json()
        if r.status_code == 200 and resp.get("status") == "success":
            _active_prometheus_url = url
            return jsonify({"status": "connected", "url": url})
        return jsonify({"status": "error",
                        "message": "Prometheus responded but returned an unexpected status"})
    except requests.exceptions.Timeout:
        return jsonify({"status": "error",
                        "message": f"Timed out after 3s — is Prometheus reachable at {url}?"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@app.route("/api/metrics_query", methods=["POST"])
def api_metrics_query():
    data = request.get_json() or {}
    query = data.get("query", "").strip()
    if not query:
        return jsonify({"error": "No query provided"}), 400
    try:
        r = requests.get(
            f"{_active_prometheus_url}/api/v1/query",
            params={"query": query},
            timeout=5
        )
        resp = r.json()
        if resp.get("status") != "success":
            return jsonify({"error": resp.get("error", "Prometheus error")}), 400

        results = []
        for item in resp["data"]["result"]:
            results.append({
                "labels": item.get("metric", {}),
                "value":  item["value"][1]
            })
        return jsonify({"results": results, "query": query})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Main ──────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    init_db()
    print(f"MCI Dashboard starting on http://0.0.0.0:{DASHBOARD_PORT}")
    print(f"Prometheus: {PROMETHEUS_URL}")
    print(f"Monitoring: namespace={NAMESPACE}, container={CONTAINER}")
    app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=True)
