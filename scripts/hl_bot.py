#!/usr/bin/env python3
"""HL bot: private engine control endpoint and separate Telegram controller.

Python standard library only. Never reads trading credentials or submits orders.
"""
import calendar
from decimal import Decimal
import hmac
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import sys
import time
import threading
import subprocess
import signal
import shutil
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.parse import urlparse


RAILWAY_API = "https://backboard.railway.com/graphql/v2"
AUTO_ALERT_INTERVAL = 14 * 60
STATUS_MAX_AGE = 120
BOOT_TIMEOUT = 900


def verification_checks(envelope):
    status = envelope.get('status') or {}
    streaming = status.get('streaming') or {}
    recon = status.get('reconciliation') or {}
    return {'process_and_snapshot': envelope.get('engine_alive') is True and envelope.get('stale') is False,
            'health': status.get('healthy') is True and status.get('fatal_stop') is False,
            'execution': status.get('execution_state') == 'NORMAL',
            'reconciliation': recon.get('recovery_pending') is False and recon.get('verified_through_unix_ms') is not None,
            'dlq': recon.get('dlq_depth') == 0,
            'sources': streaming.get('source_healthy') is True and streaming.get('live_state_confirmed') == status.get('candidate_count') and bool(status.get('candidate_count')),
            'cohort_committed': streaming.get('scan_commits',0) > 0,
            'persistence': status.get('persistence_failures') == 0 and status.get('source_persistence_failures') == 0}


def read_metadata(path):
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def lock_owner(root, proc_locks=Path("/proc/locks")):
    """Read the kernel's lock table; a PID file alone does not prove ownership."""
    try:
        st = (root / "state/.observer-state.lock").stat()
        for line in proc_locks.read_text().splitlines():
            fields = line.split()
            if len(fields) < 8 or fields[1:4] != ["FLOCK", "ADVISORY", "WRITE"]:
                continue
            major, minor, inode = fields[5].split(":")
            if (int(major, 16), int(minor, 16), int(inode)) == (os.major(st.st_dev), os.minor(st.st_dev), st.st_ino):
                return {"state": "held", "pid": int(fields[4])}
        return {"state": "unheld", "pid": None}
    except (OSError, ValueError):
        return {"state": "unverified", "pid": None}


def process_history(root, now):
    path = root / "failure/process-events.jsonl"
    try:
        events = [json.loads(line) for line in path.read_text().splitlines()]
        recent = [e for e in events if now-86400 <= e["time"] <= now]
        crashes = sum(e["event"] == "unexpected_exit" for e in recent)
        unexplained = sum(e["event"] == "unclean_predecessor" for e in recent)
        return {"crashes_24h": crashes, "crash_rate_per_hour_24h": crashes/24,
                "unexplained_stops_24h": unexplained,
                "coverage_seconds": 0, "coverage_basis": "external monitor required"}
    except (OSError, ValueError, KeyError):
        return {"crashes_24h": None, "crash_rate_per_hour_24h": None,
                "unexplained_stops_24h": None, "coverage_seconds": 0}


def record_process_event(root, run_id, event, exit_code=None):
    root = Path(root)
    path = root / "failure/process-events.jsonl"
    now = int(time.time())
    events = []
    if not path.exists():
        events.append({"event": "monitor_started", "time": now, "run_id": run_id})
        previous = root / "failure/last-exit.meta"
        if previous.exists():
            meta = read_metadata(previous)
            if meta.get("stop_requested") != "1":
                events.append({"event": "unexpected_exit", "run_id": meta["run_id"],
                    "time": calendar.timegm(time.strptime(meta["exited_at"], "%Y-%m-%dT%H:%M:%SZ")),
                    "exit_code": int(meta["exit_code"]), "imported": True})
    elif event == "start":
        # A container kill can prevent the wrapper from writing an exit. Never
        # silently report zero crashes when the previous run has no end record.
        history = [json.loads(line) for line in path.read_text().splitlines()]
        starts = [e for e in history if e["event"] == "start"]
        if starts and not any(e["run_id"] == starts[-1]["run_id"] and e["event"] in ("expected_stop", "unexpected_exit") for e in history):
            events.append({"event": "unclean_predecessor", "time": now, "run_id": starts[-1]["run_id"]})
    events.append({"event": event, "time": now, "run_id": run_id, "exit_code": exit_code})
    if event == "unexpected_exit":
        archive = root / "incident-history" / run_id
        archive.mkdir(parents=True, exist_ok=True)
        for name in ['process.stderr', 'current-run.meta', 'last-exit.meta']:
            source = root / 'failure' / name
            if source.exists(): shutil.copy2(source, archive / name)
        memory = Path('/sys/fs/cgroup/memory.events')
        if memory.exists(): (archive / 'memory.events').write_text(memory.read_text())
    with path.open("a") as handle:
        for item in events:
            handle.write(json.dumps(item, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def post(url, body, token=None, timeout=35, project_token=None):
    headers = {"Content-Type": "application/json", "User-Agent": "HL-bot/1.0"}
    if project_token:
        headers["Project-Access-Token"] = project_token
    elif token:
        headers["Authorization"] = "Bearer " + token
    request = Request(url, json.dumps(body).encode(), headers, method="POST")
    with urlopen(request, timeout=timeout) as response:
        return json.load(response)


def durable_write(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def supervise():
    """Keep control alive across engine exits; every restart is paused."""
    root = Path(os.environ.get('SU6_DATA_ROOT', '/data/system-v1'))
    control = EngineControl(root)
    if os.environ.get('HL_CONTROL_TOKEN'):
        threading.Thread(target=serve, daemon=True).start()
    stop = threading.Event()
    for sig in [signal.SIGTERM, signal.SIGINT]:
        signal.signal(sig, lambda *_: stop.set())
    if os.environ.get('HL_MAINTENANCE') == '1':
        print('maintenance=true trading=false', flush=True)
        stop.wait()
        return
    attempt = 0
    recovery_started = time.monotonic()
    recovery_deadline = int(time.time())+BOOT_TIMEOUT
    while not stop.is_set():
        started = time.monotonic()
        verification = {'status':'WARMING_UP','started_at':int(time.time()),'deadline':recovery_deadline,
                        'reason':'replacement_boot_requires_verification','resume_criteria':'all verification checks pass; live execution then follows configured risk limits',
                        'checks':{},'verified_at':None}
        if started-recovery_started >= BOOT_TIMEOUT:
            verification.update(status='FAILED',reason='recovery_budget_exhausted; acceptance_clock_not_started',resume_criteria='repair failure and deploy a verified release; control remains available')
            print(json.dumps({'event':'recovery_budget_exhausted','alert':True,**verification}),flush=True)
            durable_write(control.control/'boot-verification.json',json.dumps(verification))
            stop.wait()
            return
        print(json.dumps({'event':'boot_paused','alert':True,**verification}),flush=True)
        durable_write(control.control/'boot-verification.json',json.dumps(verification))
        child = subprocess.Popen(['/app/bin/run_su6_railway.sh'], env={**os.environ, 'HL_SUPERVISED':'1'})
        watchdog = False
        while child.poll() is None and not stop.wait(2):
            if verification['status'] == 'WARMING_UP':
                try: checks = verification_checks(control.status())
                except Exception: checks = {'diagnostics':False}
                verification['checks'] = checks
                if time.monotonic()-recovery_started >= BOOT_TIMEOUT:
                    verification['status'] = 'FAILED'
                    verification['reason'] = 'warmup_exceeded_900_seconds; acceptance_clock_not_started'
                elif all(checks.values()):
                    verification.update(status='VERIFIED',verified_at=int(time.time()),reason='first_boot_checks_passed; live_execution_eligible')
                if verification['status'] != 'WARMING_UP':
                    print(json.dumps({'event':'boot_verification','alert':True,**verification}),flush=True)
                    durable_write(control.control/'boot-verification.json',json.dumps(verification))
            if time.monotonic()-started < 180: continue
            try:
                age = time.time()-(root/'runtime/rolling-status.json').stat().st_mtime
            except OSError:
                age = 181
            if age > 180:
                watchdog = True
                print(json.dumps({'event':'watchdog_stall','time':time.time(),'snapshot_age_s':age,'alert':True}), flush=True)
                record_process_event(root, str(child.pid), 'watchdog_stall')
                break
        if child.poll() is None:
            child.terminate()
            try: child.wait(timeout=60)
            except subprocess.TimeoutExpired:
                # The engine lock prevents a replacement writer until the
                # old process is gone. Kill the recorded engine before retry.
                meta = read_metadata(root/'failure/current-run.meta')
                try: os.kill(int(meta['engine_pid']), signal.SIGKILL)
                except ProcessLookupError: pass
                child.kill()
                child.wait()
        if stop.is_set(): return
        if verification['status'] == 'VERIFIED':
            recovery_started = time.monotonic()
            recovery_deadline = int(time.time())+BOOT_TIMEOUT
        attempt = 1 if time.monotonic()-started > 300 else attempt+1
        delay = min(30, 2 ** min(attempt, 5))
        print(json.dumps({'event':'engine_restart','time':time.time(),'exit_code':child.returncode,'watchdog':watchdog,'backoff_s':delay,'alert':True}), flush=True)
        record_process_event(root, str(child.pid), 'restart_scheduled', child.returncode)
        stop.wait(delay)


def record_observation(db, envelope, now):
    """The independent bot measures wall-clock coverage, including outages."""
    db.execute('CREATE TABLE IF NOT EXISTS observations (time INTEGER PRIMARY KEY, sample TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS monitor_events (time INTEGER, event TEXT, detail TEXT)')
    ensure_acceptance_table(db)
    previous = db.execute('SELECT MAX(time) FROM observations').fetchone()[0]
    values = dict(db.execute("SELECT key,value FROM state WHERE key IN ('coverage_seconds','gap_seconds','monitor_started')"))
    delta = now-previous if previous is not None else 0
    gap = delta if delta > 45 or delta < 0 else 0
    covered = int(values.get('coverage_seconds',0)) + (delta if 0 <= delta <= 45 else 0)
    gaps = int(values.get('gap_seconds',0)) + max(0,gap)
    status = envelope.get('status', {})
    recon = status.get('reconciliation') or {}
    sample = {k:envelope.get(k) for k in ['stale','stale_reasons','age_seconds','observed_at_utc','engine_alive','pause_requested']}
    sample['operations'] = envelope.get('operations', {})
    sample['reconciliation'] = recon
    sample['execution_state'] = status.get('execution_state')
    sample['elapsed_seconds'] = status.get('elapsed_seconds')
    sample['signal_freshness'] = status.get('streaming')
    sample['model_epoch'] = status.get('mfce_model_epoch')
    sample['mfce'] = {k:v for k,v in (status.get('mfce') or {}).items() if k != 'policy_outputs'}
    sample['economics'] = next((e for e in status.get('economics',[]) if e['horizon']=='since_process_start'), None)
    sample['verification'] = envelope.get('verification')
    run = sample['operations'].get('run_id')
    checks = verification_checks(envelope)
    gate = envelope.get('verification') or {}
    reset = envelope.get('clean_reset') or {}
    ready = bool(run) and envelope.get('pause_requested') is False and all(checks.values()) and gate.get('status') == 'VERIFIED' and reset.get('action') == 'reset'
    with db:
        db.execute('INSERT OR REPLACE INTO observations VALUES (?,?)', (now,json.dumps(sample)))
        db.executemany('INSERT OR REPLACE INTO state VALUES (?,?)', [('coverage_seconds',str(covered)),('gap_seconds',str(gaps)),('monitor_started', values.get('monitor_started',str(now)))])
        if gap: db.execute('INSERT INTO monitor_events VALUES (?,?,?)',(now,'monitor_gap',str(delta)))
        active = db.execute("SELECT window_id,run,started FROM acceptance_runs WHERE state='RUNNING'").fetchall()
        for window_id, previous_run, started in active:
            if gap or not ready or previous_run != run:
                db.execute("UPDATE acceptance_runs SET state='FAILED',reason=?,ended=? WHERE window_id=?",(json.dumps({'gap':gap,'checks':checks,'run_changed':previous_run!=run}),now,window_id))
            elif now-started >= 86400:
                db.execute("UPDATE acceptance_runs SET state='PASSED',reason='24h observed without gate failure',ended=? WHERE window_id=?",(now,window_id))
        # Require an independently observed interval before starting the clock.
        if (ready and 0 < delta <= 45 and not gap and not active
                and isinstance(gate.get('verified_at'),int)
                and gate['verified_at'] <= now
                and gate['verified_at'] <= gate.get('deadline',0)):
            db.execute("INSERT OR IGNORE INTO acceptance_runs VALUES (?,?,?,'RUNNING','first verified independent observation',NULL)",(f"{run}@{now}",run,now))
        cursor = db.execute('SELECT window_id,run,started,state,reason,ended FROM acceptance_runs ORDER BY started DESC LIMIT 1')
        acceptance = cursor.fetchone()
    if acceptance: envelope['acceptance'] = dict(zip(['window_id','run','started','state','reason','ended'],acceptance))
    envelope.setdefault('operations', {}).update(coverage_seconds=covered,gap_seconds=gaps,coverage_basis='independent bot samples; max interval 45s')
    return bool(gap)


def ensure_acceptance_table(db):
    db.execute('CREATE TABLE IF NOT EXISTS acceptance_runs (window_id TEXT PRIMARY KEY, run TEXT NOT NULL, started INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL, ended INTEGER)')
    columns = db.execute('PRAGMA table_info(acceptance_runs)').fetchall()
    names = [column[1] for column in columns]
    pk_name = next((column[1] for column in columns if column[5]), None)
    if 'window_id' in names and pk_name == 'window_id':
        return
    db.execute('ALTER TABLE acceptance_runs RENAME TO acceptance_runs_legacy')
    db.execute('CREATE TABLE acceptance_runs (window_id TEXT PRIMARY KEY, run TEXT NOT NULL, started INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL, ended INTEGER)')
    legacy = [column[1] for column in db.execute('PRAGMA table_info(acceptance_runs_legacy)').fetchall()]
    if {'run','started','state','reason','ended'}.issubset(set(legacy)):
        db.execute("INSERT OR IGNORE INTO acceptance_runs(window_id,run,started,state,reason,ended) SELECT run || '@' || started, run, started, state, reason, ended FROM acceptance_runs_legacy WHERE run IS NOT NULL AND started IS NOT NULL")
    db.execute('DROP TABLE acceptance_runs_legacy')


class EngineControl:
    def __init__(self, root):
        self.root = Path(root)
        self.control = self.root / "operator"
        if self.control.is_symlink():
            raise ValueError("operator directory must not be a symlink")
        self.control.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.pause_path = self.control / "paused"

    def status(self):
        path = self.root / "runtime/rolling-status.json"
        # Read data and metadata from the same inode across atomic replacement.
        with path.open() as handle:
            mtime = os.fstat(handle.fileno()).st_mtime
            status = json.load(handle)
        now = time.time()
        age = now - mtime
        # A previous deployment's snapshot is stale even when only seconds old.
        metadata = read_metadata(self.root / "failure/current-run.meta")
        # calendar.timegm is independent of the host's local timezone.
        started = calendar.timegm(time.strptime(metadata["started_at"], "%Y-%m-%dT%H:%M:%SZ"))
        pid = int(metadata["engine_pid"])
        try:
            os.kill(pid, 0)
            alive = True
        except ProcessLookupError:
            alive = False
        ownership = lock_owner(self.root)
        reasons = []
        if age > STATUS_MAX_AGE or age < -5:
            reasons.append("snapshot_age_invalid")
        if mtime < started:
            reasons.append("previous_process_snapshot")
        if not alive:
            reasons.append("engine_not_alive")
        if ownership != {"state": "held", "pid": pid}:
            reasons.append("engine_lock_not_verified")
        recon = status.get('reconciliation') or {}
        watermark = recon.get('verified_through_unix_ms')
        if watermark is None or not 0 <= now-watermark/1000 < STATUS_MAX_AGE or recon.get('recovery_pending') is not False:
            reasons.append('account_reconciliation_unverified')
        diagnostic = self.root/'failure/process.stderr'
        with diagnostic.open('rb') if diagnostic.exists() else open(os.devnull,'rb') as handle:
            handle.seek(max(0,os.fstat(handle.fileno()).st_size-1048576))
            risk_alerts = [line for line in handle.read().decode(errors='replace').splitlines()
                if 'alert=true' in line]
        return {"status": status, "age_seconds": max(0, round(age)),
                "verification": json.loads((self.control/'boot-verification.json').read_text()) if (self.control/'boot-verification.json').exists() else {'status':'UNVERIFIED'},
                "clean_reset": json.loads((self.control/'reset-receipt.json').read_text()) if (self.control/'reset-receipt.json').exists() else None,
                "observed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(mtime)),
                "stale": bool(reasons), "stale_reasons": reasons,
                "engine_alive": alive, "pause_requested": os.path.lexists(self.pause_path),
                "operations": {**process_history(self.root, now), "lock": ownership,
                               "run_id": metadata.get("run_id"), "deployment_id": metadata.get("deployment_id"),
                               "recent_alert_count":len(risk_alerts), "last_alert":risk_alerts[-1] if risk_alerts else None,
                               "dlq_depth": (status.get("reconciliation") or {}).get("dlq_depth"), "dlq_state": "durable_reconciliation_quarantine"}}

    def pause(self):
        durable_write(self.pause_path, "operator_pause\n")
        return {"pause_requested": True,
                "message": "Pause persisted. New risk blocked at the next submission check; already-sent orders may fill. Reconciliation and reduce-only actions remain available."}

    def signals(self):
        envelope = self.status()
        path = self.root / "runtime/rolling-execution-journal.json"
        journal = json.loads(path.read_text()) if path.exists() else {}
        envelope["lifecycle"] = journal.get("lifecycle", [])[-60:]
        path = self.root / "state/live/live-trading-state.json"
        ledger = json.loads(path.read_text()) if path.exists() else {}
        envelope["verified_fills"] = [{key: f.get(key) for key in
            ("asset", "side", "reduce_only", "filled_quantity", "average_fill_price", "occurred_at")}
            for f in ledger.get("verified_fills", [])[-10:]]
        return envelope

    def resume(self):
        self.pause_path.unlink(missing_ok=True)
        fd = os.open(self.control, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return {"pause_requested": False, "message": "Pause cleared. Normal source, exchange, and strategy gates still apply."}


def serve():
    token = os.environ["HL_CONTROL_TOKEN"]
    if len(token) < 32:
        raise ValueError("HL_CONTROL_TOKEN must be at least 32 characters")
    control = EngineControl(os.environ.get("SU6_DATA_ROOT", "/data/system-v1"))

    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            supplied = self.headers.get("Authorization", "").encode()
            if not hmac.compare_digest(supplied, ("Bearer " + token).encode()):
                self.send_error(401)
                return
            actions = {"/status": control.status, "/health": control.status, "/signals": control.signals, "/pause": control.pause, "/resume": control.resume}
            if self.path not in actions:
                self.send_error(404)
                return
            try:
                with lock:
                    result = actions[self.path]()
                body = json.dumps(result).encode()
            except Exception as error:
                result = {"status": {}, "stale": True, "stale_reasons": ["snapshot_unavailable:"+type(error).__name__],
                    "age_seconds": None, "observed_at_utc": None, "engine_alive": None,
                    "pause_requested": os.path.lexists(control.pause_path),
                    "operations": process_history(control.root, time.time())}
                body = json.dumps(result).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    class Server(ThreadingHTTPServer):
        address_family = socket.AF_INET6

    # Bind IPv6 for Railway private networking. Do not attach a public domain.
    Server(("::", int(os.environ.get("HL_CONTROL_PORT", "8787"))), Handler).serve_forever()


def operational_lines(envelope):
    ops = envelope.get("operations", {})
    recon = envelope.get("status", {}).get("reconciliation") or {}
    watermark = recon.get("verified_through_unix_ms")
    lag = max(0, time.time()-watermark/1000) if isinstance(watermark, (int, float)) else None
    return [f"Crashes (24h): {ops.get('crashes_24h')}; unexplained stops: {ops.get('unexplained_stops_24h')}; run: {ops.get('run_id')}",
        f"Diagnostic alerts (retained stderr): {ops.get('recent_alert_count')}; last={ops.get('last_alert')}",
        f"Monitoring: covered {ops.get('coverage_seconds', 0)}s; gap {ops.get('gap_seconds', 'not yet measured')}s; basis: {ops.get('coverage_basis', 'external monitor required')}",
        f"Reconciliation: source_ms={watermark}; age_s={round(lag) if lag is not None else 'unknown'}; recovery_pending={recon.get('recovery_pending')}; reason={recon.get('last_error')}",
        f"Retries: {recon.get('attempts', 'unknown')}/5; recovery_ms={recon.get('last_recovery_ms')}; DLQ depth={recon.get('dlq_depth', 'not configured')}; alert={bool(recon.get('dlq_depth'))}",
        f"Lock: {ops.get('lock')}; engine_alive={envelope.get('engine_alive')}"]


def render_status(envelope, horizon="since_process_start"):
    status = envelope.get("status", {})
    stale = envelope.get("stale", True)
    source = envelope.get('observed_at_utc', 'unknown')
    age = envelope.get('age_seconds')
    reasons = list(envelope.get('stale_reasons') or [])
    blockers = list(status.get('execution_blockers') or [])
    if envelope.get('pause_requested'): blockers.append('operator_paused')
    recon = status.get('reconciliation') or {}
    watermark = recon.get('verified_through_unix_ms')
    if watermark is None or not 0 <= time.time()-watermark/1000 < STATUS_MAX_AGE or recon.get('recovery_pending') is not False:
        blockers.append('exchange_reconciliation_unverified_or_stale')
    account_valid = not stale and 'exchange_reconciliation_unverified_or_stale' not in blockers
    economy = next((e for e in status.get('economics', []) if e['horizon'] == horizon), {})
    stream = status.get('streaming') or {}
    lines = [f"HL bot — {'STALE / UNVERIFIED' if stale else 'snapshot current'}",
        f"All snapshot fields: source={source}; age_s={age}; reason={','.join(reasons) or 'none'}",
        f"Healthy: {account_valid and bool(status.get('healthy'))}; last execution mode={status.get('execution_state', 'unknown')}; blockers={','.join(blockers) or 'none'}",
        f"New-risk pause={envelope.get('pause_requested')}; strategy targets ready={status.get('strategy_target_execution_enabled')}",
        f"Positions (last observed; account validity={not stale and recon.get('recovery_pending') is False}): {status.get('exchange_risk_positions')}",
        f"Signals: confirmed={stream.get('live_state_confirmed')}/{status.get('candidate_count')}; pending={stream.get('coverage_reconciliation_wallets_pending')}; stream gaps={stream.get('gaps')}",
        f"Model epoch={status.get('mfce_model_epoch')}; last evaluated outputs={len((status.get('mfce') or {}).get('policy_outputs', []))}"]
    lines += operational_lines(envelope)
    lines.append(f"Boot verification: {envelope.get('verification', {'status':'UNVERIFIED'})}; acceptance: {envelope.get('acceptance', 'clock not started')}")
    lines += [f"Financial validity={'UNVERIFIED / invalid for decisions' if stale or recon.get('recovery_pending') is not False else 'reconciled account; inherited exposure excluded from strategy qualification'}; source={source}; age_s={age}",
        f"Window={horizon}; settled equity={status.get('settled_equity')}; marked equity={status.get('marked_equity')}"]
    for label, key in [('Executions','executions'), ('Closures','closures'), ('Gross PnL','gross_pnl'),
        ('Net PnL','net_pnl'), ('Fees','fees'), ('Slippage','slippage'), ('Funding','funding'),
        ('Profit factor','profit_factor'), ('Sharpe (5m)','annualized_sharpe_5m'),
        ('Drawdown','maximum_drawdown'), ('Strategy win rate','strategy_net_win_rate')]:
        lines.append(f"{label}: {economy.get(key, 'unavailable')}")
    lines.append(f"PF state={economy.get('profit_factor_state', 'unavailable')}; Sharpe samples={economy.get('sharpe_sample_count')}; strategy closures={economy.get('strategy_closures')}; manual closures={economy.get('manual_closures')}")
    return "\n".join(lines)


def render_signals(envelope):
    status = envelope.get('status', {})
    outputs = (status.get('mfce') or {}).get('policy_outputs', [])
    lines = [f"HL signals — {'STALE / UNVERIFIED' if envelope.get('stale') else 'snapshot current'}; execution={status.get('execution_state', 'unknown')}",
        f"All signals: source={envelope.get('observed_at_utc', 'unknown')}; age_s={envelope.get('age_seconds')}; reason={envelope.get('stale_reasons')}",
        f"Global readiness: {status.get('execution_blockers', [])}; {'operator_paused' if envelope.get('pause_requested') else ''}",
        f"Evaluated={len(outputs)}; survivors={sum(bool(p.get('admitted')) for p in outputs)}; submitted fills are reported separately"]
    recon = status.get('reconciliation') or {}
    watermark = recon.get('verified_through_unix_ms')
    if watermark is None or not 0 <= time.time()-watermark/1000 < STATUS_MAX_AGE or recon.get('recovery_pending') is not False:
        lines.append('exchange_reconciliation_unverified_or_stale')
    for p in sorted(outputs, key=lambda p: str(p.get('asset')))[:12]:
        at = p.get('signal_observed_at')
        age = round(time.time()-at/1000) if isinstance(at,(int,float)) else 'unknown'
        lines.append(f"{p.get('asset')} {p.get('direction')} | source_ms={at} age_s={age} | admitted={p.get('admitted')} | target={p.get('target_notional')} | net_q50_bps={p.get('net_q50_bps')} | conservative_bps={p.get('conservative_edge_bps')} | cost_bps={p.get('friction_bps')} | reason={p.get('reason')}")
    lines.append(f"Last action outcomes: {envelope.get('lifecycle', [])[-3:]}")
    lines.append(f"Last ledger fills (account validity as above): {envelope.get('verified_fills', [])[-3:]}")
    return "\n".join(lines)


def render_exchange_costs(fills, cutoff):
    recent = [f for f in fills if f.get("time", 0) >= cutoff]
    liquidations = [f for f in recent if f.get("liquidation")]
    ordinary = [f for f in recent if not f.get("liquidation")]
    total = lambda rows, key: sum((Decimal(str(f.get(key, "0"))) for f in rows), Decimal(0))
    gross, fees = total(ordinary, "closedPnl"), total(ordinary, "fee")
    net = gross - fees
    fee_share = f"{fees / -net * 100:.2f}%" if net < 0 else "not applicable (no net loss)"
    return ("HL bot — available exchange-fill costs (funding excluded)\n"
            f"Non-liquidation fills: {len(ordinary)} | gross realized PnL: {gross} | fees: {fees} | gross minus fees: {net}\n"
            f"Fees / non-liquidation net loss: {fee_share}\n"
            f"Liquidation-tagged fills: {len(liquidations)} | gross realized PnL: {total(liquidations, 'closedPnl')} | fees: {total(liquidations, 'fee')}\n"
            "Opening fees are included; fill counts are not closed-trade win rates. Exchange history may be bounded; use /metrics for settled episode results.")


class Bot:
    def __init__(self):
        self.telegram_url = "https://api.telegram.org/bot" + os.environ["HL_TELEGRAM_BOT_TOKEN"] + "/"
        self.user = int(os.environ["HL_TELEGRAM_USER_ID"])
        if self.user <= 0:
            raise ValueError("Set your positive numeric Telegram user ID")
        self.control_url = os.environ["HL_ENGINE_CONTROL_URL"].rstrip("/")
        url = urlparse(self.control_url)
        if url.scheme != "https" and not (url.scheme == "http" and (url.hostname or "").endswith(".railway.internal")):
            raise ValueError("Control URL must use HTTPS or Railway private networking")
        self.token = os.environ["HL_CONTROL_TOKEN"]
        if len(self.token) < 32:
            raise ValueError("HL_CONTROL_TOKEN must be at least 32 characters")
        self.railway_token = os.environ.get("HL_RAILWAY_API_TOKEN")
        self.railway_project_token = os.environ.get("HL_RAILWAY_PROJECT_TOKEN")
        self.scope = {key: os.environ.get(env) for key, env in [
            ("projectId", "HL_RAILWAY_PROJECT_ID"), ("serviceId", "HL_RAILWAY_ENGINE_SERVICE_ID"),
            ("environmentId", "HL_RAILWAY_ENVIRONMENT_ID")]}
        path = Path(os.environ.get("HL_BOT_STATE_DB", "/data/hl-bot/state.sqlite"))
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS sent_messages (id INTEGER PRIMARY KEY, sent_at INTEGER NOT NULL)")
        self.pending = None
        self.started = int(time.time())
        # Reserve a full quiet interval on first install, based on the latest
        # prior bot message when available. The watermark survives redeploys.
        with self.db:
            latest = self.db.execute("SELECT MAX(sent_at) FROM sent_messages").fetchone()[0]
            self.db.execute("INSERT OR IGNORE INTO state VALUES ('auto_alert_at', ?)", (str(latest or self.started),))

    def automatic_notice(self, fingerprint, notice, now=None, urgent=False):
        now = int(time.time() if now is None else now)
        encoded = json.dumps(fingerprint, sort_keys=True)
        # Reserve before sending; immediate incident alerts bypass digest cadence.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            values = dict(self.db.execute("SELECT key, value FROM state WHERE key IN ('auto_alert_at','auto_alert_fingerprint')"))
            changed = encoded != values.get("auto_alert_fingerprint")
            if now-int(values.get("auto_alert_at", now)) < AUTO_ALERT_INTERVAL and not (urgent and changed):
                self.db.rollback()
                return False
            self.db.executemany("INSERT OR REPLACE INTO state VALUES (?, ?)",
                                [("auto_alert_at", str(now)), ("auto_alert_fingerprint", encoded)])
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise
        self.send(notice)
        return True

    def telegram(self, method, payload):
        response = post(self.telegram_url + method, payload)
        if not response.get("ok"):
            raise RuntimeError("Telegram API failed")
        return response["result"]

    def send(self, message):
        result = self.telegram("sendMessage", {"chat_id": self.user, "text": message[:4000]})
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO sent_messages VALUES (?, ?)", (result["message_id"], int(time.time())))

    def clear(self, command_id):
        self.pending = None
        # Owner-authorized reset of this private bot conversation, including
        # legacy updates predating message tracking. Never another chat.
        end = int(command_id or 0)
        if end <= 0:
            self.send("Clear requires a Telegram message ID.")
            return
        failed = False
        for start in range(max(1, end-5000), end+1, 100):
            try:
                self.telegram("deleteMessages", {"chat_id": self.user, "message_ids": list(range(start, min(start+100, end+1)))})
            except Exception:
                failed = True
        with self.db:
            self.db.execute("DELETE FROM sent_messages WHERE id <= ?", (end,))
        self.send("Chat reset requested. Telegram may retain messages older than 48 hours." + (" Some messages could not be removed." if failed else ""))
        try:
            self.send(render_status(self.control("status")))
        except Exception:
            self.send("Engine is currently unavailable; use /status after deployment finishes.")

    def control(self, action):
        envelope = post(self.control_url + "/" + action, {}, self.token, timeout=10)
        if action in ("status", "signals"):
            # A newly written JSON file can still contain old account truth
            # while reconciliation is failing. Its mtime is not sufficient.
            reconciliation = envelope["status"].get("reconciliation") or {}
            watermark = reconciliation.get("verified_through_unix_ms")
            if (watermark is None or not 0 <= time.time()-watermark/1000 < STATUS_MAX_AGE
                    or reconciliation.get("recovery_pending") is not False):
                envelope["stale"] = True
                envelope.setdefault("stale_reasons", []).append("account_reconciliation_unverified")
        return envelope

    def monitor_status(self):
        last_error = None
        for attempt in range(2):
            try:
                return self.control("status")
            except Exception as error:
                last_error = error
                if attempt == 0:
                    time.sleep(1)
        raise last_error

    def railway(self, query, variables):
        if not (self.railway_project_token or self.railway_token) or not all(self.scope.values()):
            raise ValueError("Railway controls are not configured")
        response = post(RAILWAY_API, {"query": query, "variables": variables}, self.railway_token,
                        project_token=self.railway_project_token)
        if response.get("errors") or not response.get("data"):
            raise RuntimeError("Railway API failed")
        return response["data"]

    def deployment(self):
        result = self.railway("query($input: DeploymentListInput!) { deployments(input: $input, first: 1) { edges { node { id status } } } }", {"input": self.scope})
        return result["deployments"]["edges"][0]["node"]

    def handle(self, message):
        if (message.get("from", {}).get("id") != self.user
                or message.get("chat", {}).get("id") != self.user
                or message.get("chat", {}).get("type") != "private"
                or message.get("from", {}).get("is_bot")
                or message.get("forward_origin") or message.get("forward_date")):
            return
        date = message.get("date", 0)
        if date < self.started or not 0 <= time.time() - date <= 120:
            return
        words = message.get("text", "").split()
        if not words:
            return
        command = words[0].split("@")[0].lower()
        if command in ("/redeploy", "/stop") and not (self.railway_project_token or self.railway_token):
            self.send("Railway deployment controls are not configured. Store the production-scoped project token as HL_RAILWAY_PROJECT_TOKEN in hl-bot's Railway secrets. Keep it out of chat. No deployment action was attempted.")
            return
        if command == "/clear":
            self.clear(message.get("message_id"))
        elif command == "/signals":
            self.send(render_signals(self.control("signals")))
        elif command in ("/start", "/help"):
            self.send("HL bot\n/status\n/signals — surviving candidates and execution outcomes\n/clear — clear recent chat and show fresh status\n/costs — last 24h fees excluding liquidation, funding excluded\n/metrics [last_24h|last_7d|last_30d|lifetime|since_process_start]\n/pause — persistently block new risk\n/resume — clear pause, retain normal gates\n/redeploy — redeploy latest engine source\n/stop — stop engine service (positions remain on exchange)\n/cancel — discard pending control\nControls require a fresh one-use /confirm code. This bot stays online independently.")
        elif command in ("/status", "/metrics"):
            horizon = words[1] if len(words) == 2 else "since_process_start"
            if horizon not in ("last_24h", "last_7d", "last_30d", "lifetime", "since_process_start"):
                self.send("Unknown metrics window. Use /help.")
                return
            self.send(render_status(self.control("status"), horizon))
        elif command == "/costs":
            account = os.environ.get("HL_EXCHANGE_PUBLIC_ACCOUNT", "")
            if len(account) != 42 or not account.startswith("0x"):
                self.send("Public exchange account is not configured for cost diagnostics.")
                return
            fills = post("https://api.hyperliquid.xyz/info", {"type": "userFills", "user": account})
            self.send(render_exchange_costs(fills, int(time.time()*1000)-86_400_000))
        elif command == "/pause":
            self.pending = None
            self.send(self.control("pause")["message"])
        elif command in ("/resume", "/redeploy", "/stop"):
            action = command[1:]
            deployment = self.deployment() if action in ("redeploy", "stop") else None
            code = secrets.token_hex(4)
            self.pending = (action, code, time.monotonic() + 60, deployment)
            detail = f" on deployment {deployment['id']} ({deployment['status']})" if deployment else ""
            self.send(f"Confirm {action}{detail} with /confirm {code} within 60s. Stop does not close positions; redeploy reuses that deployment's source and resets live source confirmations.")
        elif command == "/cancel":
            self.pending = None
            self.send("Pending control cancelled.")
        elif command == "/confirm":
            pending, self.pending = self.pending, None
            if not pending or len(words) != 2 or not hmac.compare_digest(words[1], pending[1]) or time.monotonic() > pending[2]:
                self.send("Invalid or expired confirmation. Request the control again.")
                return
            action, _, _, deployment = pending
            if action == "resume":
                self.send(self.control("resume")["message"])
            else:
                if self.deployment() != deployment:
                    self.send("Deployment changed. Request the control again.")
                    return
                if action == "redeploy":
                    result = self.railway("mutation($id: String!) { deploymentRedeploy(id: $id) { id status } }", {"id": deployment["id"]})
                    self.send("Redeploy requested: " + json.dumps(result["deploymentRedeploy"]))
                else:
                    result = self.railway("mutation($id: String!) { deploymentStop(id: $id) }", {"id": deployment["id"]})
                    self.send("Railway stop result: " + str(result["deploymentStop"]) + ". Exchange positions remain; verify in Hyperliquid.")
        else:
            self.send("Unknown command. Use /help.")

    def run(self):
        row = self.db.execute("SELECT value FROM state WHERE key='offset'").fetchone()
        offset = int(row[0]) if row else 0
        next_check = 0
        while True:
            try:
                try:
                    updates = self.telegram("getUpdates", {"offset": offset, "timeout": 10, "allowed_updates": ["message"]})
                except Exception:
                    updates = []
                for update in updates:
                    offset = max(offset, update["update_id"] + 1)
                    # Persist before effects: never automatically replay a destructive
                    # request after a crash or ambiguous external API response.
                    with self.db:
                        self.db.execute("INSERT OR REPLACE INTO state VALUES ('offset', ?)", (str(offset),))
                    try:
                        self.handle(update.get("message", {}))
                    except Exception:
                        self.send("Request failed or outcome unknown. Check /status and Railway before retrying. No automatic control retry.")
                if time.monotonic() >= next_check:
                    next_check = time.monotonic() + 15
                    recordable = True
                    try:
                        envelope = self.monitor_status()
                        status = envelope["status"]
                        ops = envelope.get("operations", {})
                        recon = status.get("reconciliation") or {}
                        watermark = recon.get("verified_through_unix_ms")
                        fingerprint = (envelope["stale"], status.get("healthy"), status.get("execution_state"), envelope["pause_requested"], tuple(status.get("execution_blockers", [])),
                                       ops.get("crashes_24h"), ops.get("unexplained_stops_24h"), ops.get("run_id"),
                                       watermark is not None and time.time()-watermark/1000 >= 120,
                                       ops.get("dlq_depth"), (ops.get("lock") or {}).get("state"), ops.get('recent_alert_count'))
                        with self.db:
                            self.db.execute("INSERT OR REPLACE INTO state VALUES ('last_engine_envelope',?)", (json.dumps(envelope),))
                    except Exception:
                        recordable = False
                        fingerprint = ("unreachable",)
                        cached = self.db.execute("SELECT value FROM state WHERE key='last_engine_envelope'").fetchone()
                        envelope = json.loads(cached[0]) if cached else {'status':{},'age_seconds':None}
                        envelope.update(stale=True,stale_reasons=['control_endpoint_unreachable; last retained snapshot'])
                        if envelope.get('observed_at_utc'):
                            envelope['age_seconds']=int(time.time()-calendar.timegm(time.strptime(envelope['observed_at_utc'],'%Y-%m-%dT%H:%M:%SZ')))
                    gap = record_observation(self.db, envelope, int(time.time())) if recordable else False
                    notice = render_status(envelope)
                    last_run = self.db.execute("SELECT value FROM state WHERE key='last_observed_run'").fetchone()
                    run = str((envelope.get('operations') or {}).get('run_id'))
                    restart = last_run is not None and last_run[0] != run
                    with self.db:
                        self.db.execute("INSERT OR REPLACE INTO state VALUES ('last_observed_run',?)",(run,))
                    urgent = gap or restart or envelope.get('stale') or bool((envelope.get('operations') or {}).get('recent_alert_count')) or bool((envelope.get('status',{}).get('reconciliation') or {}).get('dlq_depth'))
                    self.automatic_notice(fingerprint, notice, urgent=urgent)
            except Exception:
                # Do not log exception URLs: Telegram URLs contain the bot secret.
                print("hl_bot_poll_failed=true retry_in_seconds=5", flush=True)
                time.sleep(5)


if __name__ == "__main__":
    os.umask(0o077)
    if sys.argv[1:2] == ["supervise"]:
        try:
            supervise()
        except Exception as error:
            print(json.dumps({'event':'supervisor_fatal','time':time.time(),'alert':True,
                'trading':False,'reason':repr(error),'recovery':'Railway ALWAYS restart; boot verification required'}),file=sys.stderr,flush=True)
            raise SystemExit(1)
    elif sys.argv[1:2] == ["record-process-event"]:
        record_process_event(*sys.argv[2:])
    elif sys.argv[1:] == ["serve"]:
        serve()
    elif sys.argv[1:] == ["identify"]:
        # Setup only: read the owner's /start before running the polling service.
        result = post("https://api.telegram.org/bot" + os.environ["HL_TELEGRAM_BOT_TOKEN"] + "/getUpdates",
                      {"timeout": 0, "allowed_updates": ["message"]})
        if not result.get("ok"):
            raise SystemExit("Telegram setup lookup failed")
        owners = {(m["from"]["id"], m["from"].get("username", ""))
                  for u in result["result"] for m in [u.get("message", {})]
                  if m.get("chat", {}).get("type") == "private" and not m.get("from", {}).get("is_bot")}
        for user_id, username in sorted(owners):
            print(f"user_id={user_id} username={username}")
        if not owners:
            print("Send /start to your new bot, then rerun identify before starting the controller.")
    elif sys.argv[1:] == ["bot"]:
        Bot().run()
    else:
        raise SystemExit("usage: hl_bot.py [serve|bot|identify]")
