#!/usr/bin/env python3
"""Protocol runner for the Kafka vs RabbitMQ POC (lab brief, section 5).

Each subcommand runs one experiment family against the running compose stack and writes
results/<experiment>/<run_id>.json. The apps write audit CSVs to results/audit/<run_id>/,
which are reconciled here (loss, duplicates, per-order ordering).
Stdlib only.
"""
import argparse
import base64
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RESULTS = REPO / "results"
AUDIT = RESULTS / "audit"
PROJECT = "poc-message-breaker"
PRODUCER = {"kafka": ("order-producer-kafka", 8080), "rabbitmq": ("order-producer-rabbitmq", 8083)}
CONSUMER = {"kafka": "order-consumer-kafka", "rabbitmq": "order-consumer-rabbitmq"}
BROKER = {"kafka": "kafka", "rabbitmq": "rabbitmq"}
TOPIC, DLT, GROUP = "orders.events", "orders.events.DLT", "poc-kafka-consumer"
ORDERS = {"10k": 3334, "100k": 33334, "1M": 333334}  # 3 events per order
STAMP = time.strftime("%Y%m%d-%H%M%S")
ROLE_SUFFIX = re.compile(r"-[0-9a-f]{12}\.csv$")
SAMPLE = re.compile(r"^([A-Za-z_:][\w:]*)(\{.*\})?\s+(\S+)$")
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def now():
    return time.time()


def sh(args, env=None, check=True, timeout=900):
    r = subprocess.run(args, cwd=REPO, env={**os.environ, **(env or {})}, capture_output=True, text=True,
                       timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} -> {r.stderr.strip() or r.stdout.strip()}")
    return r.stdout


def compose(*args, env=None, check=True):
    return sh(["docker", "compose", *args], env=env, check=check)


def http(url, method="GET", body=None, timeout=60):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    if ":15672/" in url:
        req.add_header("Authorization", "Basic " + base64.b64encode(b"poc:poc").decode())
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def wait_up(url, timeout=240):
    end = now() + timeout
    while now() < end:
        try:
            if '"UP"' in http(f"{url}/actuator/health", timeout=5):
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"{url} not UP")


def wait_healthy(container, timeout=300):
    end = now() + timeout
    while now() < end:
        if sh(["docker", "inspect", "--format", "{{.State.Health.Status}}", container], check=False).strip() == "healthy":
            return
        time.sleep(2)
    raise TimeoutError(f"{container} not healthy")


# ---------- Prometheus text parsing ----------

def parse_prometheus(text):
    out = {}
    for line in text.splitlines():
        m = SAMPLE.match(line) if line and not line.startswith("#") else None
        if m:
            key = (m.group(1), tuple(sorted(LABEL.findall(m.group(2) or ""))))
            out[key] = out.get(key, 0.0) + float(m.group(3))
    return out


def merge(snapshots):
    merged = {}
    for s in snapshots:
        for k, v in s.items():
            merged[k] = merged.get(k, 0.0) + v
    return merged


def series(samples, name, **labels):
    want = {k: str(v) for k, v in labels.items()}
    return {k: v for k, v in samples.items()
            if k[0] == name and all(dict(k[1]).get(a) == b for a, b in want.items())}


def total(samples, name, **labels):
    return sum(series(samples, name, **labels).values())


def quantile(samples, base, q, **labels):
    buckets = {}
    for (_, lbls), v in series(samples, base + "_bucket", **labels).items():
        le = dict(lbls)["le"]
        b = math.inf if le == "+Inf" else float(le)
        buckets[b] = buckets.get(b, 0.0) + v
    if not buckets:
        return None
    items = sorted(buckets.items())
    count = items[-1][1]
    if count <= 0:
        return None
    target = q * count
    prev_b, prev_c = 0.0, 0.0
    for b, c in items:
        if c >= target:
            if math.isinf(b):
                return prev_b
            if c == prev_c:
                return b
            return prev_b + (b - prev_b) * (target - prev_c) / (c - prev_c)
        prev_b, prev_c = b, c
    return prev_b


def ms(v):
    return None if v is None else round(v * 1000, 2)


def latency_block(samples, base, transport, **extra):
    return {f"p{int(q*100)}_ms": ms(quantile(samples, base, q, transport=transport, **extra))
            for q in (0.50, 0.95, 0.99)}


# ---------- services ----------

def producer_url(t):
    return f"http://localhost:{PRODUCER[t][1]}"


def start_producer(t, run_id):
    compose("up", "-d", "--no-deps", "--force-recreate", PRODUCER[t][0], env={"POC_AUDIT_RUN": run_id})
    wait_up(producer_url(t))


def load_start(t, **kw):
    body = {"itemsPerOrder": 2, "paymentFailureRate": 0.05, "outOfStockRate": 0.05, **kw}
    return json.loads(http(f"{producer_url(t)}/api/load/start", "POST", body))


def load_status(t):
    return json.loads(http(f"{producer_url(t)}/api/load/status"))


def wait_load_done(t, timeout=3600):
    end = now() + timeout
    while now() < end:
        st = load_status(t)
        if not st["running"] and st["inFlight"] == 0:
            return st
        time.sleep(0.5)
    raise TimeoutError("producer did not finish")


def stop_consumers(t):
    compose("stop", CONSUMER[t], check=False)


def consumer_urls(t):
    urls = []
    for cid in compose("ps", "-q", CONSUMER[t], check=False).split():
        out = sh(["docker", "port", cid, "8080"], check=False).strip()
        if out:
            urls.append(f"http://localhost:{out.splitlines()[0].rsplit(':', 1)[1]}")
    return urls


def start_consumers(t, run_id, n=1, delay=0, failure=0.0, stream=False):
    svc = CONSUMER[t]
    env = {"POC_AUDIT_RUN": run_id, "PROCESSING_DELAY_MS": str(delay),
           "PROCESSING_FAILURE_RATE": str(failure), "STREAM_CONSUMER_ENABLED": "true" if stream else "false"}
    compose("up", "-d", "--no-deps", "--force-recreate", "--scale", f"{svc}={n}", svc, env=env)
    urls = consumer_urls(t)
    if len(urls) != n:
        raise RuntimeError(f"expected {n} consumer instances, found {len(urls)}")
    for u in urls:
        wait_up(u)
    return urls


def consumer_snapshot(t):
    per = [parse_prometheus(http(f"{u}/actuator/prometheus")) for u in consumer_urls(t)]
    return merge(per), per


# ---------- broker state ----------

def kafka_backlog():
    out = compose("exec", "-T", BROKER["kafka"], "sh", "-c",
                  f"env -u KAFKA_OPTS /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 "
                  f"--describe --group {GROUP}", check=False)
    rows = [p for p in (l.split() for l in out.splitlines()) if len(p) >= 6 and p[1] == TOPIC]
    if len(rows) < 3:
        return None
    lag = 0
    for p in rows:
        lag += int(p[5]) if p[5].isdigit() else (int(p[4]) if p[4].isdigit() else 0)
    return lag


def rabbit_queues():
    return {q["name"]: q for q in json.loads(http("http://localhost:15672/api/queues/%2F"))}


def rabbit_backlog():
    return sum(q.get("messages", 0) for n, q in rabbit_queues().items() if n.startswith("orders.events.q."))


def backlog(t):
    return kafka_backlog() if t == "kafka" else rabbit_backlog()


def wait_drain(t, timeout=3600, every=1.0):
    start = now()
    series_ = []
    while True:
        b = backlog(t)
        elapsed = now() - start
        if b is not None:
            series_.append((round(elapsed, 1), b))
        if b == 0 and series_:
            return round(elapsed, 2), series_
        if elapsed > timeout:
            raise TimeoutError(f"{t} backlog not drained (last={b})")
        time.sleep(every)


def dlq_count(t):
    if t == "kafka":
        out = compose("exec", "-T", BROKER["kafka"], "sh", "-c",
                      f"env -u KAFKA_OPTS /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell "
                      f"--bootstrap-server kafka:9092 --topic {DLT}", check=False)
        return sum(int(l.split(":")[-1]) for l in out.splitlines() if l.count(":") == 2)
    return rabbit_queues().get("orders.dlq", {}).get("messages", 0)


def disk_bytes(t):
    path = "/tmp/kraft-combined-logs" if t == "kafka" else "/var/lib/rabbitmq"
    out = compose("exec", "-T", BROKER[t], "sh", "-c", f"du -sk {path}", check=False).split()
    return int(out[0]) * 1024 if out and out[0].isdigit() else None


def kafka_retention():
    out = compose("exec", "-T", "kafka", "sh", "-c",
                  "env -u KAFKA_OPTS /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --describe "
                  "--entity-type topics --entity-name orders.events --all", check=False)
    m = re.search(r"retention\.ms=(\S+)", out)
    return m.group(1) if m else None


# ---------- resources ----------

def mib(s):
    m = re.match(r"([\d.]+)\s*([KMG]i?B|B)", s.strip())
    if not m:
        return 0.0
    factor = {"B": 1 / 1024**2, "KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}
    return float(m.group(1)) * factor.get(m.group(2), 1)


class Sampler(threading.Thread):
    """Integrates docker stats CPU% over time (CPU-seconds) and tracks peak memory per service."""

    def __init__(self):
        super().__init__(daemon=True)
        self._stop = threading.Event()
        self.cpu_s, self.mem_peak_mib, self.samples = {}, {}, 0

    def run(self):
        last = now()
        while not self._stop.is_set():
            out = sh(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"], check=False)
            t = now()
            dt, last = t - last, t
            mem = {}
            for line in out.splitlines():
                parts = line.split("\t")
                if len(parts) != 3 or not parts[0].startswith(PROJECT + "-"):
                    continue
                svc = re.sub(r"-\d+$", "", parts[0][len(PROJECT) + 1:])
                self.cpu_s[svc] = self.cpu_s.get(svc, 0.0) + float(parts[1].rstrip("%") or 0) / 100 * dt
                mem[svc] = mem.get(svc, 0.0) + mib(parts[2].split("/")[0])
            for svc, m in mem.items():
                self.mem_peak_mib[svc] = max(self.mem_peak_mib.get(svc, 0.0), m)
            self.samples += 1
            time.sleep(1)

    def stop(self):
        self._stop.set()
        self.join()

    def result(self):
        return {"cpu_seconds": {k: round(v, 1) for k, v in self.cpu_s.items()},
                "mem_peak_mib": {k: round(v, 1) for k, v in self.mem_peak_mib.items()},
                "docker_samples": self.samples}


# ---------- audit reconciliation ----------

def audit_rows(run_id):
    for f in sorted((AUDIT / run_id).glob("*.csv")):
        role = ROLE_SUFFIX.sub("", f.name)
        with f.open() as fh:
            for line in fh:
                p = line.rstrip("\n").split(",")
                if len(p) == 7:
                    yield role, p


def analyse(run_id, t):
    produced, consumed_ids, consumed_rows = set(), set(), 0
    ok_first = {}
    for role, (ts, eid, oid, seq, _typ, mode, status) in audit_rows(run_id):
        if role == f"producer-{t}":
            produced.add(eid)
        elif role == f"consumer-{t}" and mode == "live":
            consumed_rows += 1
            consumed_ids.add(eid)
            if status == "ok":
                key, tsi = (oid, int(seq)), int(ts)
                if key not in ok_first or tsi < ok_first[key]:
                    ok_first[key] = tsi
    steps_by_order = {}
    for (oid, seq), tsi in ok_first.items():
        steps_by_order.setdefault(oid, {})[seq] = tsi
    checked = bad = ties = 0
    for steps in steps_by_order.values():
        checked += 1
        violated = False
        for a, b in ((1, 2), (2, 3)):
            if a in steps and b in steps:
                if steps[b] < steps[a]:
                    violated = True
                elif steps[b] == steps[a]:
                    ties += 1
        bad += violated
    return {"events_produced": len(produced), "events_consumed_distinct": len(consumed_ids),
            "consumer_rows": consumed_rows, "duplicate_rows": consumed_rows - len(consumed_ids),
            "lost": len(produced - consumed_ids), "unexpected": len(consumed_ids - produced),
            "orders_checked": checked, "orders_out_of_order": bad, "same_ms_order_pairs": ties}


def first_row_ts(run_id, role, after_ms, status=None):
    best = None
    for r, (ts, _e, _o, _s, _t, mode, st) in audit_rows(run_id):
        if r == role and int(ts) >= after_ms and (status is None or st == status):
            best = int(ts) if best is None else min(best, int(ts))
    return best


# ---------- result plumbing ----------

def write_result(exp, run_id, data):
    path = RESULTS / exp / f"{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
    log(f"wrote {path.relative_to(REPO)}")
    return data


def produce_then_drain(t, run_id, orders, consumers=1, delay=0, failure=0.0, stream=False, payload=0,
                       invalid=0.0, rate=20000, in_flight=2000, live=False):
    """live=False: publish with no consumer (broker backlog), then measure consumption (drain).
    live=True: consumers run while publishing, then measure the remaining drain."""
    if live:
        start_consumers(t, run_id, consumers, delay, failure, stream)
    else:
        stop_consumers(t)
    start_producer(t, run_id)
    sampler = Sampler()
    sampler.start()
    t0 = now()
    load_start(t, ratePerSecond=rate, maxOrders=orders, maxInFlight=in_flight,
               payloadPaddingBytes=payload, invalidRate=invalid)
    wait_load_done(t)
    produce_s = round(now() - t0, 2)
    prod = parse_prometheus(http(f"{producer_url(t)}/actuator/prometheus"))
    published = total(prod, "poc_producer_published_total")
    if not live:
        start_consumers(t, run_id, consumers, delay, failure, stream)
    drain_s, series_ = wait_drain(t)
    merged, per = consumer_snapshot(t)
    sampler.stop()
    ok = total(merged, "poc_consumer_processed_total", transport=t, mode="live", status="ok")
    inv = total(merged, "poc_consumer_processed_total", transport=t, mode="live", status="invalid")
    return {
        "run_id": run_id, "transport": t, "orders": orders, "consumers": consumers, "live": live,
        "delay_ms": delay, "failure_rate": failure, "invalid_rate": invalid, "payload_bytes": payload,
        "events_published": int(published), "events_consumed_ok": int(ok), "events_consumed_invalid": int(inv),
        "orders_outcome": {k: int(total(prod, "poc_producer_orders_total", outcome=k))
                           for k in ("completed", "invalid_sent", "failed", "dropped")},
        "produce_seconds": produce_s,
        "produce_rate_msg_s": round(published / produce_s, 1) if produce_s else None,
        "drain_seconds": drain_s,
        "drain_rate_msg_s": round((ok + inv) / drain_s, 1) if drain_s else None,
        "per_instance_consumed": [int(total(p, "poc_consumer_processed_total", transport=t, mode="live",
                                             status="ok") + total(p, "poc_consumer_processed_total",
                                                                  transport=t, mode="live", status="invalid"))
                                  for p in per],
        "consumer_retryable_attempts": int(total(merged, "poc_consumer_processed_total", transport=t,
                                                 mode="live", status="retryable")),
        "dlq_counter": int(total(merged, "poc_dlq_published_total", transport=t)),
        "latency": {"e2e": latency_block(merged, "poc_e2e_latency_seconds", t, mode="live"),
                    "processing": latency_block(merged, "poc_consumer_processing_seconds", t, mode="live"),
                    "producer_confirm": latency_block(prod, "poc_producer_confirm_seconds", t)},
        "resources": sampler.result(),
        "backlog_series": series_[:: max(1, len(series_) // 300)],
        "audit": analyse(run_id, t),
    }


def warmup(t):
    produce_then_drain(t, f"warmup-{t}-{STAMP}", orders=500, live=True, rate=200, in_flight=200)


# ---------- experiments ----------

def exp_debit(a):
    for t in a.transports:
        warmup(t)
        for label, orders in ORDERS.items():
            for rep in range(a.reps):
                run_id = f"debit-{t}-{label}-r{rep}-{STAMP}"
                # RabbitMQ closes channels (406 TIMEOUT WAITING FOR ACK) above ~2000 unconfirmed publishes.
                r = produce_then_drain(t, run_id, orders, rate=20000, in_flight=100)
                r["label"] = label
                write_result("debit", run_id, r)


def exp_latency(a):
    for t in a.transports:
        warmup(t)
        for payload in (0, 1024, 10240):
            for rate in (50, 500):
                run_id = f"latency-{t}-p{payload}-r{rate}-{STAMP}"
                r = produce_then_drain(t, run_id, orders=rate * a.duration, live=True, payload=payload,
                                       rate=rate, in_flight=500)
                r["achieved_order_rate"] = round(r["orders"] / a.duration, 1)
                write_result("latency", run_id, r)


def exp_concurrency(a):
    for t in a.transports:
        warmup(t)
        for n in (1, 3, 5):
            for rep in range(a.reps):
                run_id = f"concurrency-{t}-n{n}-r{rep}-{STAMP}"
                write_result("concurrency", run_id,
                             produce_then_drain(t, run_id, orders=10000, consumers=n, rate=20000,
                                                in_flight=100))


def exp_slow(a):
    for t in a.transports:
        warmup(t)
        run_id = f"slow-{t}-{STAMP}"
        start_consumers(t, run_id, 1, delay=20)
        start_producer(t, run_id)
        t0 = now()
        load_start(t, ratePerSecond=100, maxOrders=6000, maxInFlight=500)
        trace = []
        while True:
            st = load_status(t)
            b = backlog(t)
            trace.append({"t": round(now() - t0, 1), "backlog": b, "inFlight": st["inFlight"]})
            log(f"{t} slow: t={trace[-1]['t']} backlog={b} inFlight={st['inFlight']}")
            if not st["running"] and st["inFlight"] == 0:
                break
            time.sleep(5)
        prod_end = round(now() - t0, 1)
        drain_s, _ = wait_drain(t)
        merged, _ = consumer_snapshot(t)
        peak = max((x["backlog"] for x in trace if x["backlog"] is not None), default=None)
        write_result("slow", run_id, {
            "run_id": run_id, "transport": t, "delay_ms": 20, "offered_msg_s": 300, "orders": 6000,
            "production_end_s": prod_end, "backlog_peak": peak, "reprise_after_production_s": drain_s,
            "events_consumed": int(total(merged, "poc_consumer_processed_total", transport=t, mode="live",
                                         status="ok")),
            "latency": {"e2e": latency_block(merged, "poc_e2e_latency_seconds", t, mode="live")},
            "trace": trace[:: max(1, len(trace) // 200)], "audit": analyse(run_id, t)})


def exp_crash(a):
    for t in a.transports:
        warmup(t)
        run_id = f"crash-{t}-{STAMP}"
        start_consumers(t, run_id, 1)
        start_producer(t, run_id)
        load_start(t, ratePerSecond=50, maxOrders=3000, maxInFlight=500)
        time.sleep(20)
        t_kill = now()
        compose("kill", "-s", "SIGKILL", CONSUMER[t])
        log(f"{t}: consumer killed (SIGKILL)")
        time.sleep(10)
        compose("start", CONSUMER[t])
        wait_up(consumer_urls(t)[0])
        t_restart = now()
        log(f"{t}: consumer restarted")
        wait_load_done(t)
        drain_s, _ = wait_drain(t)
        restart_ms = int(t_restart * 1000)
        first_ok = first_row_ts(run_id, f"consumer-{t}", restart_ms, "ok")
        write_result("crash", run_id, {
            "run_id": run_id, "transport": t, "kill_at_s": 20, "restart_after_s": 10,
            "recovery_first_ok_after_restart_s": None if first_ok is None else round((first_ok - restart_ms) / 1000, 2),
            "reprise_backlog_zero_after_restart_s": round(now() - t_restart, 2),
            "drain_after_end_s": drain_s, "audit": analyse(run_id, t)})


def exp_broker(a):
    for t in a.transports:
        warmup(t)
        run_id = f"broker-{t}-{STAMP}"
        start_consumers(t, run_id, 1)
        start_producer(t, run_id)
        t0 = now()
        load_start(t, ratePerSecond=50, maxOrders=3000, maxInFlight=500)
        time.sleep(20)
        t_stop = now()
        compose("stop", BROKER[t])
        log(f"{t}: broker stopped")
        time.sleep(20)
        compose("start", BROKER[t])
        wait_healthy(f"{PROJECT}-{BROKER[t]}-1")
        t_up = now()
        registry_restarted = False
        if t == "kafka":
            try:
                wait_healthy(f"{PROJECT}-schema-registry-1", timeout=60)
            except TimeoutError:
                compose("restart", "schema-registry")
                wait_healthy(f"{PROJECT}-schema-registry-1")
                registry_restarted = True
        log(f"{t}: broker healthy again")
        st = wait_load_done(t, timeout=1800)
        drain_s, _ = wait_drain(t)
        prod = parse_prometheus(http(f"{producer_url(t)}/actuator/prometheus"))
        first_confirm_after_stop = first_row_ts(run_id, f"producer-{t}", int(t_stop * 1000))
        first_confirm_after_up = first_row_ts(run_id, f"producer-{t}", int(t_up * 1000))
        write_result("broker", run_id, {
            "run_id": run_id, "transport": t, "broker_down_s": round(t_up - t_stop, 1),
            "schema_registry_restarted": registry_restarted,
            "outage_to_first_confirm_s": None if first_confirm_after_stop is None else
            round((first_confirm_after_stop - int(t_stop * 1000)) / 1000, 2),
            "first_confirm_after_recovery_s": None if first_confirm_after_up is None else
            round((first_confirm_after_up - int(t_up * 1000)) / 1000, 2),
            "orders_outcome": {k: int(total(prod, "poc_producer_orders_total", outcome=k))
                               for k in ("completed", "invalid_sent", "failed", "dropped")},
            "producer_nacks": int(total(prod, "poc_producer_nacks_total")),
            "producer_wall_s": round(now() - t0, 1), "drain_after_end_s": drain_s,
            "audit": analyse(run_id, t)})


def exp_faults(a):
    for t in a.transports:
        warmup(t)
        run_id = f"faults-{t}-{STAMP}"
        dlq_before = dlq_count(t)
        r = produce_then_drain(t, run_id, orders=600, failure=0.05, invalid=0.05, rate=10, in_flight=100, live=True)
        time.sleep(6)  # management stats refresh every 5 s
        r["dlq_broker_delta"] = dlq_count(t) - dlq_before
        r["label"] = "transient 5% + invalid 5%"
        write_result("faults", run_id, r)


def exp_replay(a):
    for t in a.transports:
        warmup(t)
        run_id = f"replay-{t}-{STAMP}"
        disk0 = disk_bytes(t)
        t_start_ms = int(now() * 1000)
        r = produce_then_drain(t, run_id, orders=10000, stream=(t == "rabbitmq"), live=True, rate=20000,
                               in_flight=100)
        disk1 = disk_bytes(t)
        url = consumer_urls(t)[0]
        rep = json.loads(http(f"{url}/api/replay?timestamp={t_start_ms}", "POST", timeout=1800))
        r["replay"] = rep
        r["replay_matches_published"] = rep["processed"] + rep["failed"] == r["events_published"]
        r["disk_bytes_per_event"] = None if None in (disk0, disk1) else round((disk1 - disk0) / r["events_published"], 1)
        if t == "kafka":
            r["history"] = {"retention_ms": kafka_retention(), "events_in_topic": int(
                sum(int(l.split(":")[-1]) for l in compose("exec", "-T", "kafka", "sh", "-c",
                    "env -u KAFKA_OPTS /opt/kafka/bin/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell "
                    "--bootstrap-server kafka:9092 --topic orders.events", check=False).splitlines()
                    if l.count(":") == 2))}
        else:
            time.sleep(6)
            qs = rabbit_queues()
            r["history"] = {"quorum_ready_after_consumption": qs["orders.events.q.0"].get("messages", 0)
                            + qs["orders.events.q.1"].get("messages", 0) + qs["orders.events.q.2"].get("messages", 0),
                            "stream_messages": qs["orders.events.stream"].get("messages", 0)}
        write_result("replay", run_id, r)


def exp_ordering(a):
    for t in a.transports:
        warmup(t)
        for n in (1, 3):
            run_id = f"ordering-{t}-n{n}-{STAMP}"
            r = produce_then_drain(t, run_id, orders=3000, consumers=n, failure=0.05, rate=100, in_flight=500,
                                   live=True)
            write_result("ordering", run_id, r)


EXPERIMENTS = {"debit": exp_debit, "latency": exp_latency, "concurrency": exp_concurrency, "slow": exp_slow,
               "crash": exp_crash, "broker": exp_broker, "faults": exp_faults, "replay": exp_replay,
               "ordering": exp_ordering}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment", choices=sorted(EXPERIMENTS))
    parser.add_argument("--transports", nargs="+", default=["kafka", "rabbitmq"], choices=["kafka", "rabbitmq"])
    parser.add_argument("--reps", type=int, default=1)
    parser.add_argument("--duration", type=int, default=40, help="seconds per latency run")
    a = parser.parse_args()
    EXPERIMENTS[a.experiment](a)


if __name__ == "__main__":
    main()
