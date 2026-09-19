import importlib.util
import fcntl
import json
import os
import sqlite3
from pathlib import Path
import tempfile
import time
import unittest
import subprocess
import sys
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('hl_bot', Path(__file__).parents[1] / 'scripts/hl_bot.py')
hl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hl)


class RailwayAuthentication(unittest.TestCase):
    def test_project_token_uses_scoped_header_without_bearer(self):
        with patch.object(hl, 'urlopen') as request:
            request.return_value.__enter__.return_value.read.return_value = b'{}'
            hl.post(hl.RAILWAY_API, {}, token='account-secret', project_token='project-secret')
            headers = dict(request.call_args.args[0].header_items())
            self.assertEqual(headers.get('Project-access-token'), 'project-secret')
            self.assertNotIn('Authorization', headers)

    def test_account_token_uses_bearer_when_project_token_absent(self):
        with patch.object(hl, 'urlopen') as request:
            request.return_value.__enter__.return_value.read.return_value = b'{}'
            hl.post(hl.RAILWAY_API, {}, token='account-secret')
            headers = dict(request.call_args.args[0].header_items())
            self.assertEqual(headers.get('Authorization'), 'Bearer account-secret')
            self.assertNotIn('Project-access-token', headers)


class CostDiagnostics(unittest.TestCase):
    def test_signals_distinguish_survivors_pending_and_verified_fills(self):
        result = hl.render_signals({'stale': False, 'age_seconds': 4, 'status': {
            'execution_blockers': [], 'unresolved_root_details': [{'asset':'xyz:SKHY','lifecycle':'pending_live_book'}],
            'mfce': {'policy_outputs': [dict(asset='xyz:SKHY', direction='long', admitted=True,
                target_notional='12', policy_state='explore', net_q50_bps='25', conservative_edge_bps='2', friction_bps='10'),
                dict(asset='xyz:CBRS', admitted=False, reason='negative_edge')]}},
            'verified_fills': [], 'lifecycle': [{'asset':'APT','root_planned_cloid':'x','requested_notional':'11','outcome':'exchange_rejected'}]})
        self.assertIn('survivors=1', result)
        self.assertIn('xyz:SKHY long', result)
        self.assertIn('execution=unknown', result)
        self.assertIn('submitted fills are reported separately', result)
        self.assertIn('xyz:CBRS', result)
        self.assertIn('negative_edge', result)

    def test_liquidation_is_separated_from_fees_and_time_window(self):
        result = hl.render_exchange_costs([
            {'time':2, 'closedPnl':'1', 'fee':'2'},
            {'time':2, 'closedPnl':'-80', 'fee':'0.2', 'liquidation':{'method':'market'}},
            {'time':0, 'closedPnl':'-99', 'fee':'4'},
        ], 1)
        self.assertIn('Non-liquidation fills: 1 | gross realized PnL: 1 | fees: 2 | gross minus fees: -1', result)
        self.assertIn('net loss: 200.00%', result)
        self.assertIn('Liquidation-tagged fills: 1 | gross realized PnL: -80 | fees: 0.2', result)


class Controls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.control = hl.EngineControl(self.root)

    def test_pause_survives_new_control_and_resume(self):
        self.control.pause()
        self.assertTrue(hl.EngineControl(self.root).pause_path.exists())
        self.control.resume()
        self.control.resume()
        self.assertFalse(self.control.pause_path.exists())

    def test_status_detects_previous_run_and_staleness(self):
        (self.root / 'runtime').mkdir()
        (self.root / 'failure').mkdir()
        (self.root / 'state').mkdir()
        lock = (self.root / 'state/.observer-state.lock').open('w')
        self.addCleanup(lock.close)
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = self.root / 'runtime/rolling-status.json'
        path.write_text(json.dumps({'reconciliation':{'verified_through_unix_ms':int(time.time()*1000),'recovery_pending':False}}))
        metadata = self.root / 'failure/current-run.meta'
        metadata.write_text(f'started_at=2000-01-01T00:00:00Z\nengine_pid={os.getpid()}\n')
        with patch.object(hl, 'lock_owner', return_value={'state':'held', 'pid':os.getpid()}):
            self.assertFalse(self.control.status()['stale'])
        os.utime(path, (time.time()-121, time.time()-121))
        self.assertTrue(self.control.status()['stale'])
        path.write_text('{}')
        metadata.write_text(f'started_at=2100-01-01T00:00:00Z\nengine_pid={os.getpid()}\n')
        self.assertTrue(self.control.status()['stale'])

    def test_no_symlink_operator_root(self):
        self.control.control.rmdir()
        self.control.control.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError):
            hl.EngineControl(self.root)


class Commands(unittest.TestCase):
    def setUp(self):
        self.bot = hl.Bot.__new__(hl.Bot)
        self.bot.user = 1234
        self.bot.started = int(time.time())-1
        self.bot.pending = None
        self.bot.railway_project_token = 'fixture-project-token'
        self.bot.railway_token = None
        self.bot.db = sqlite3.connect(':memory:')
        self.addCleanup(self.bot.db.close)
        self.bot.db.execute('CREATE TABLE sent_messages (id INTEGER PRIMARY KEY, sent_at INTEGER)')
        self.messages = []
        self.actions = []
        self.bot.send = self.messages.append
        self.bot.control = lambda action: self.actions.append(action) or {'message': action}
        self.bot.deployment = lambda: {'id': 'engine-deployment', 'status': 'SUCCESS'}
        self.bot.railway = lambda query, variables: self.actions.append((query, variables)) or {'deploymentStop': True}

    def message(self, text, **changes):
        value = {'from': {'id': 1234}, 'chat': {'id': 1234, 'type': 'private'},
                 'date': int(time.time()), 'text': text}
        value.update(changes)
        self.bot.handle(value)

    def test_unauthorized_group_forwarded_and_old_commands_ignored(self):
        for changes in [{'from': {'id': 999}}, {'chat': {'id': 1234, 'type': 'group'}},
                        {'forward_origin': {'type': 'user'}}, {'date': int(time.time())-121},
                        {'from': {'id': 1234, 'is_bot': True}}]:
            self.message('/pause', **changes)
        self.assertEqual(self.actions, [])
        self.assertEqual(self.messages, [])

    def test_pause_immediate_resume_requires_confirmation(self):
        self.message('/pause')
        self.message('/resume')
        self.assertEqual(self.actions, ['pause'])
        code = self.bot.pending[1]
        self.message('/confirm ' + code)
        self.message('/confirm ' + code)
        self.assertEqual(self.actions, ['pause', 'resume'])

    def test_stop_pins_deployment_and_requires_one_use_code(self):
        self.message('/stop')
        code = self.bot.pending[1]
        self.assertEqual(self.actions, [])
        self.message('/confirm '+code)
        self.message('/confirm '+code)
        self.assertEqual(len(self.actions), 1)
        self.assertEqual(self.actions[0][1], {'id': 'engine-deployment'})

    def test_missing_railway_token_is_explicit_and_never_attempts_control(self):
        self.bot.railway_project_token = None
        self.message('/stop')
        self.message('/redeploy')
        self.assertEqual(self.actions, [])
        self.assertIsNone(self.bot.pending)
        self.assertTrue(all('not configured' in m for m in self.messages))

    def test_changed_deployment_and_expired_code_do_not_execute(self):
        self.message('/stop')
        code = self.bot.pending[1]
        self.bot.deployment = lambda: {'id': 'new-deployment', 'status': 'SUCCESS'}
        self.message('/confirm '+code)
        self.assertEqual(self.actions, [])
        self.message('/resume')
        action, code, _, deployment = self.bot.pending
        self.bot.pending = (action, code, time.monotonic()-1, deployment)
        self.message('/confirm '+code)
        self.assertEqual(self.actions, [])

    def test_pause_cancels_pending_resume(self):
        self.message('/resume')
        code = self.bot.pending[1]
        self.message('/pause')
        self.message('/confirm '+code)
        self.assertEqual(self.actions, ['pause'])

    def test_clear_is_owner_scoped_batched_and_does_not_change_engine(self):
        deletions = []
        self.bot.telegram = lambda method, payload: deletions.append((method, payload)) or True
        self.message('/clear', message_id=207, **{'from': {'id': 999}})
        self.assertEqual(deletions, [])
        self.message('/clear', message_id=207)
        self.assertEqual(len(deletions), 3)
        self.assertTrue(all(method == 'deleteMessages' and p['chat_id'] == 1234 and len(p['message_ids']) <= 100 for method,p in deletions))
        self.assertEqual([i for _,p in deletions for i in p['message_ids']], list(range(1,208)))
        self.assertEqual(self.actions, ['status'])
        self.assertIsNone(self.bot.pending)

    def test_format_preserves_no_sample_pf_and_costs(self):
        result = hl.render_status({'stale': False, 'age_seconds': 4, 'pause_requested': False,
                                  'status': {'mfce': {'policy_outputs': [{'asset':'xyz:GOLD', 'reason':'below_exchange_minimum'}, {'asset':'BTC', 'reason':'allocation_budget_exhausted'}]}, 'hip3_source_markets_seen':122, 'hip3_fills':7, 'economics': [{'horizon':'since_process_start', 'closures':0,
                                    'profit_factor_state':'no_settled_closures', 'profit_factor':None,
                                    'gross_pnl':'3', 'net_pnl':'-1', 'fees':'4', 'net_win_rate':'0.6666666667', 'net_wins':2}]}})
        self.assertIn('snapshot current', result)
        self.assertIn('PF state=no_settled_closures', result)
        self.assertIn('Net PnL: -1', result)
        self.assertIn('Fees: 4', result)
        self.assertIn('invalid for decisions', result)



class IncidentMonitoring(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'failure').mkdir()

    def test_stale_snapshot_discloses_old_values_without_claiming_health(self):
        envelope = {'stale': True, 'age_seconds': 121, 'pause_requested': False,
            'status': {'healthy':True, 'execution_state':'NORMAL', 'settled_equity':'123456',
                       'reconciliation': {'verified_through_unix_ms':int(time.time()*1000)},
                       'mfce': {'policy_outputs': [{'admitted':True, 'target_notional':'9', 'asset':'xyz:CL'}]}}}
        for result in [hl.render_status(envelope), hl.render_signals(envelope)]:
            self.assertIn('STALE', result)
            self.assertNotIn('Healthy: True', result)
            self.assertIn('age_s=121', result)
        self.assertIn('123456', hl.render_status(envelope))
        self.assertIn('xyz:CL', hl.render_signals(envelope))

    def test_live_pause_and_reconcile_age_override_snapshot_readiness(self):
        envelope = {'stale':False, 'age_seconds':2, 'pause_requested':True,
            'status': {'healthy':True, 'execution_state':'NORMAL', 'execution_blockers':[],
                'reconciliation':{'verified_through_unix_ms':int((time.time()-121)*1000)}}}
        for result in [hl.render_status(envelope), hl.render_signals(envelope)]:
            self.assertIn('operator_paused', result)
            self.assertIn('exchange_reconciliation_unverified_or_stale', result)
            self.assertNotIn('Healthy: True', result)
            self.assertNotIn('normal gates active', result)

    def test_fresh_file_with_unreconciled_account_discloses_financial_invalidity(self):
        b = hl.Bot.__new__(hl.Bot)
        b.control_url, b.token = 'http://engine.internal', 'test'
        for reconciliation in [None,
                {'verified_through_unix_ms':int((time.time()-121)*1000), 'recovery_pending':False},
                {'verified_through_unix_ms':int(time.time()*1000), 'recovery_pending':True}]:
            envelope = {'stale':False, 'age_seconds':1, 'pause_requested':False,
                'status': {'settled_equity':'999888', 'execution_state':'NORMAL', 'reconciliation':reconciliation}}
            with patch.object(hl, 'post', return_value=envelope):
                result = b.control('status')
            self.assertTrue(result['stale'])
            rendered = hl.render_status(result)
            self.assertIn('999888', rendered)
            self.assertIn('invalid for decisions', rendered)
            self.assertIn('last execution mode=NORMAL', rendered)
            self.assertIn('account_reconciliation_unverified', rendered)

    def test_lock_owner_matches_kernel_inode_and_pid(self):
        (self.root / 'state').mkdir()
        path = self.root / 'state/.observer-state.lock'
        path.touch()
        st = path.stat()
        proc = self.root / 'locks'
        proc.write_text(f'3: FLOCK ADVISORY WRITE 42 {os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino} 0 EOF\n')
        self.assertEqual(hl.lock_owner(self.root, proc), {'state':'held','pid':42})
        proc.write_text('')
        self.assertEqual(hl.lock_owner(self.root, proc)['state'], 'unheld')

    def test_crash_history_survives_restart_and_distinguishes_clean_stop(self):
        hl.record_process_event(self.root, 'first', 'start')
        hl.record_process_event(self.root, 'first', 'expected_stop', '0')
        hl.record_process_event(self.root, 'second', 'start')
        hl.record_process_event(self.root, 'second', 'unexpected_exit', '1')
        hl.record_process_event(self.root, 'third', 'start')
        hl.record_process_event(self.root, 'fourth', 'start')
        result = hl.process_history(self.root, time.time())
        self.assertEqual(result['crashes_24h'], 1)
        self.assertEqual(result['unexplained_stops_24h'], 1)
        self.assertAlmostEqual(result['crash_rate_per_hour_24h'], 1/24)

    def test_automatic_throttle_survives_restart_ambiguity_and_clock_rollback(self):
        path = self.root / 'bot.sqlite'
        def bot():
            b = hl.Bot.__new__(hl.Bot)
            b.db = sqlite3.connect(path)
            self.addCleanup(b.db.close)
            b.db.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
            b.send = lambda notice: sent.append(notice)
            return b
        sent = []
        first = bot()
        with first.db:
            first.db.execute("INSERT INTO state VALUES ('auto_alert_at','0')")
        self.assertTrue(first.automatic_notice('healthy', 'first', 900))
        restarted = bot()
        self.assertFalse(restarted.automatic_notice('crashed', 'too soon', 1739))
        self.assertFalse(restarted.automatic_notice('crashed', 'clock moved backwards', 1))
        with patch.object(restarted, 'send', side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):
                restarted.automatic_notice('crashed', 'ambiguous', 1740)
        self.assertFalse(bot().automatic_notice('recovered', 'too soon', 2579))
        self.assertTrue(bot().automatic_notice('recovered', 'latest only', 2580))
        self.assertEqual(sent, ['first', 'latest only'])

    def test_coverage_records_outages_and_alerts_on_observation_gaps(self):
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.execute('CREATE TABLE state (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        envelope = {'status':{}, 'stale':True, 'stale_reasons':['unreachable']}
        self.assertFalse(hl.record_observation(db,envelope,100))
        self.assertFalse(hl.record_observation(db,envelope,120))
        self.assertTrue(hl.record_observation(db,envelope,200))
        self.assertEqual(envelope['operations']['coverage_seconds'],20)
        self.assertEqual(envelope['operations']['gap_seconds'],80)
        self.assertEqual(db.execute('SELECT count(*) FROM monitor_events').fetchone()[0],1)

    def test_supervisor_restarts_crashed_child_with_pause_and_backoff(self):
        app = self.root/'app/bin'
        app.mkdir(parents=True)
        wrapper = app/'run_su6_railway.sh'
        marker = self.root/'restarted'
        wrapper.write_text('#!/bin/sh\nif [ ! -f "'+str(marker)+'" ]; then touch "'+str(marker)+'"; exit 23; fi\ntrap "exit 0" TERM INT\nwhile true; do sleep 0.1; done\n')
        wrapper.chmod(0o755)
        script = self.root/'supervisor.py'
        script.write_text(Path(hl.__file__).read_text().replace('/app/bin/', str(app)+'/'))
        env = {**os.environ, 'SU6_DATA_ROOT':str(self.root)}
        env.pop('HL_CONTROL_TOKEN',None)
        env.pop('HL_MAINTENANCE',None)
        child = subprocess.Popen([sys.executable,str(script),'supervise'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            deadline=time.monotonic()+6
            while time.monotonic()<deadline:
                if marker.exists() and (self.root/'failure/process-events.jsonl').exists(): break
                time.sleep(.05)
            time.sleep(2.2)
            self.assertIsNone(child.poll())
            self.assertEqual(json.loads((self.root/'operator/boot-verification.json').read_text())['status'], 'WARMING_UP')
        finally:
            child.terminate()
            out,err=child.communicate(timeout=5)
        self.assertEqual(child.returncode,0,err.decode())
        self.assertIn('"backoff_s": 2',out.decode())
        self.assertIn('"exit_code": 23',out.decode())

    def test_supervisor_integrity_failure_emits_structured_recovery_alert(self):
        invalid = self.root/'not-a-directory'
        invalid.write_text('corrupt root')
        result = subprocess.run([sys.executable,hl.__file__,'supervise'],
            env={**os.environ,'SU6_DATA_ROOT':str(invalid)},capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,1)
        event=json.loads(result.stderr.strip())
        self.assertEqual(event['event'],'supervisor_fatal')
        self.assertTrue(event['alert'])
        self.assertFalse(event['trading'])
        self.assertIn('ALWAYS restart',event['recovery'])

    def test_diagnostic_failure_still_emits_independent_supervisor_alert(self):
        app=self.root/'app/bin';app.mkdir(parents=True)
        wrapper=app/'run_su6_railway.sh';wrapper.write_text('#!/bin/sh\nexit 23\n');wrapper.chmod(0o755)
        (self.root/'failure/process-events.jsonl').mkdir()
        script=self.root/'supervisor.py';script.write_text(Path(hl.__file__).read_text().replace('/app/bin/',str(app)+'/'))
        env={**os.environ,'SU6_DATA_ROOT':str(self.root)};env.pop('HL_MAINTENANCE',None);env.pop('HL_CONTROL_TOKEN',None)
        result=subprocess.run([sys.executable,str(script),'supervise'],env=env,capture_output=True,text=True,timeout=8)
        self.assertEqual(result.returncode,1)
        self.assertIn('"event": "engine_restart"',result.stdout)
        event=json.loads(result.stderr.strip())
        self.assertEqual(event['event'],'supervisor_fatal')
        self.assertTrue(event['alert'])

    def test_paused_boot_timeout_is_loud_and_does_not_start_clock(self):
        app=self.root/'app/bin';app.mkdir(parents=True)
        wrapper=app/'run_su6_railway.sh';wrapper.write_text('#!/bin/sh\ntrap "exit 0" TERM INT\nwhile true; do sleep .1; done\n');wrapper.chmod(0o755)
        script=self.root/'supervisor.py';script.write_text(Path(hl.__file__).read_text().replace('/app/bin/',str(app)+'/').replace('BOOT_TIMEOUT = 900','BOOT_TIMEOUT = 1'))
        env={**os.environ,'SU6_DATA_ROOT':str(self.root)};env.pop('HL_MAINTENANCE',None);env.pop('HL_CONTROL_TOKEN',None)
        child=subprocess.Popen([sys.executable,str(script),'supervise'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            time.sleep(3)
            gate=json.loads((self.root/'operator/boot-verification.json').read_text())
            self.assertEqual(gate['status'],'FAILED');self.assertIsNone(gate['verified_at'])
            self.assertIsNone(child.poll())
        finally:
            child.terminate();out,err=child.communicate(timeout=5)
        self.assertIn(b'boot_paused',out);self.assertIn(b'resume_criteria',out);self.assertIn(b'boot_verification',out)

    def test_crash_replacements_share_one_recovery_budget(self):
        app=self.root/'app/bin';app.mkdir(parents=True)
        marker=self.root/'launches'
        wrapper=app/'run_su6_railway.sh';wrapper.write_text('#!/bin/sh\necho start >> "'+str(marker)+'"\nexit 23\n');wrapper.chmod(0o755)
        script=self.root/'supervisor.py';script.write_text(Path(hl.__file__).read_text().replace('/app/bin/',str(app)+'/').replace('BOOT_TIMEOUT = 900','BOOT_TIMEOUT = 1'))
        env={**os.environ,'SU6_DATA_ROOT':str(self.root)};env.pop('HL_MAINTENANCE',None);env.pop('HL_CONTROL_TOKEN',None)
        child=subprocess.Popen([sys.executable,str(script),'supervise'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            deadline=time.monotonic()+5
            gate={}
            while time.monotonic()<deadline:
                path=self.root/'operator/boot-verification.json'
                if path.exists():gate=json.loads(path.read_text())
                if gate.get('reason')=='recovery_budget_exhausted; acceptance_clock_not_started':break
                time.sleep(.05)
            self.assertEqual(gate['reason'],'recovery_budget_exhausted; acceptance_clock_not_started')
            self.assertEqual(marker.read_text().splitlines(),['start'])
            self.assertIsNone(child.poll())
        finally:
            child.terminate();out,err=child.communicate(timeout=5)
        self.assertEqual(child.returncode,0,err)
        self.assertIn(b'recovery_budget_exhausted',out)

    def test_acceptance_clock_requires_clean_verified_boot_and_gap_starts_new_window(self):
        db=sqlite3.connect(':memory:');db.execute('CREATE TABLE state(key TEXT PRIMARY KEY,value TEXT)')
        envelope={'engine_alive':True,'stale':False,'pause_requested':False,'operations':{'run_id':'run-1'},'verification':{'status':'VERIFIED','verified_at':100,'deadline':900},
            'clean_reset':{'action':'reset'},'status':{'healthy':True,'fatal_stop':False,'execution_state':'NORMAL','strategy_target_execution_enabled':True,
            'candidate_count':375,'streaming':{'source_healthy':True,'live_state_confirmed':375,'engine_live_state_confirmed':375,'durable_baselines':375,'hydrated_source_count':375,'live_state_recovering':0,'source_history_contiguous':375,'source_history_catching_up':0,'source_history_gapped':0,'coverage_reconciliation_wallets_pending':0,'coverage_reconciliation_wallets_pending_sample':[],'source_degraded_coverage_eligible':True,'scan_commits':1,'last_scan_commit_ms':1,'gaps':0},'persistence_failures':0,'source_persistence_failures':0,
            'reconciliation':{'recovery_pending':False,'verified_through_unix_ms':100_000,'dlq_depth':0}}}
        hl.record_observation(db,envelope,100)
        self.assertIsNone(db.execute('SELECT started FROM acceptance_runs').fetchone())
        hl.record_observation(db,envelope,115)
        self.assertEqual(envelope['acceptance']['started'],115)
        hl.record_observation(db,envelope,180)
        self.assertEqual(envelope['acceptance']['state'],'FAILED')
        hl.record_observation(db,envelope,195)
        self.assertEqual(envelope['acceptance']['state'],'RUNNING')
        self.assertEqual(envelope['acceptance']['started'],195)
        self.assertEqual(db.execute("SELECT count(*) FROM acceptance_runs WHERE run='run-1'").fetchone()[0],2)

    def test_acceptance_schema_migrates_legacy_primary_key(self):
        db=sqlite3.connect(':memory:');db.execute('CREATE TABLE state(key TEXT PRIMARY KEY,value TEXT)')
        db.execute("CREATE TABLE acceptance_runs(run TEXT PRIMARY KEY, started INTEGER, state TEXT, reason TEXT, ended INTEGER)")
        db.execute("INSERT INTO acceptance_runs VALUES ('run-1',100,'FAILED','old',120)")
        envelope={'engine_alive':True,'stale':False,'pause_requested':False,'operations':{'run_id':'run-1'},'verification':{'status':'VERIFIED','verified_at':100,'deadline':900},
            'clean_reset':{'action':'reset'},'status':{'healthy':True,'fatal_stop':False,'execution_state':'NORMAL','candidate_count':375,
            'streaming':{'source_healthy':True,'live_state_confirmed':375,'engine_live_state_confirmed':375,'durable_baselines':375,'hydrated_source_count':375,'live_state_recovering':0,'source_history_contiguous':375,'source_history_catching_up':0,'source_history_gapped':0,'coverage_reconciliation_wallets_pending':0,'coverage_reconciliation_wallets_pending_sample':[],'source_degraded_coverage_eligible':True,'scan_commits':1,'last_scan_commit_ms':1,'gaps':0},'persistence_failures':0,'source_persistence_failures':0,
            'reconciliation':{'recovery_pending':False,'verified_through_unix_ms':100_000,'dlq_depth':0}}}
        hl.record_observation(db,envelope,130)
        hl.record_observation(db,envelope,145)
        rows=db.execute("SELECT window_id,run,started,state FROM acceptance_runs ORDER BY started").fetchall()
        self.assertEqual(rows[0],('run-1@100','run-1',100,'FAILED'))
        self.assertEqual(rows[1],('run-1@145','run-1',145,'RUNNING'))

    def test_monitor_status_retries_once_before_declaring_unreachable(self):
        bot=hl.Bot.__new__(hl.Bot)
        calls=[]
        def control(_):
            calls.append(1)
            if len(calls)==1:
                raise TimeoutError('transient')
            return {'status':{'reconciliation':{'verified_through_unix_ms':100_000,'recovery_pending':False}}}
        bot.control=control
        with patch.object(hl.time,'sleep') as sleep:
            self.assertIn('status',bot.monitor_status())
        sleep.assert_called_once_with(1)
        self.assertEqual(len(calls),2)


class VerifierStreamingContract(unittest.TestCase):
    """Cross-language contract: every streaming key the verifier reads must be
    present and typed. scan_commits was once required by Python but never
    serialized by Rust, making the gate permanently false with no CI signal."""

    def healthy_streaming(self, **overrides):
        base={'source_healthy':True,'live_state_confirmed':375,'engine_live_state_confirmed':375,
            'durable_baselines':375,'hydrated_source_count':375,'live_state_recovering':0,
            'source_history_contiguous':375,'source_history_catching_up':0,'source_history_gapped':0,
            'coverage_reconciliation_wallets_pending':0,'coverage_reconciliation_wallets_pending_sample':[],
            'source_degraded_coverage_eligible':True,'scan_commits':1,'last_scan_commit_ms':1,'gaps':0}
        base.update(overrides)
        return base

    def status(self, streaming):
        return {'candidate_count':375,'persistence_failures':0,'source_persistence_failures':0,
            'streaming':streaming}

    def test_strict_healthy_cohort_passes(self):
        streaming=self.healthy_streaming()
        self.assertTrue(hl.source_coverage_gate(self.status(streaming), streaming))
        self.assertTrue(hl.cohort_committed_gate(self.status(streaming), streaming))

    def test_missing_coverage_keys_fail_closed_not_open(self):
        # A dropped count key must never read as healthy; .get defaults keep this
        # fail-closed, and this test pins that behavior.
        for key in ['durable_baselines','hydrated_source_count']:
            streaming=self.healthy_streaming()
            del streaming[key]
            sources=hl.source_coverage_gate(self.status(streaming), streaming)
            self.assertFalse(sources, f'missing {key} must fail sources gate')
        # Missing both confirmed counters fails even when the cohort is loaded.
        streaming=self.healthy_streaming(live_state_confirmed=0,
            engine_live_state_confirmed=0)
        self.assertFalse(hl.source_coverage_gate(self.status(streaming), streaming))
        # Missing scan_commits does not fail: streaming recovery persists
        # per-wallet state rather than the cohort scan transaction, so the
        # fully restored durable cohort still counts as committed when
        # persistence is clean. Strict source coverage is unaffected.
        streaming=self.healthy_streaming()
        del streaming['scan_commits']
        self.assertTrue(hl.cohort_committed_gate(self.status(streaming), streaming))
        self.assertTrue(hl.source_coverage_gate(self.status(streaming), streaming))

    def test_degraded_two_stragglers_pass_but_zero_confirmed_does_not(self):
        degraded=self.healthy_streaming(source_healthy=False, live_state_confirmed=373,
            engine_live_state_confirmed=373, durable_baselines=373, hydrated_source_count=373,
            live_state_recovering=2, source_history_catching_up=2,
            coverage_reconciliation_wallets_pending=2,
            coverage_reconciliation_wallets_pending_sample=['a','b'],
            source_degraded_coverage_eligible=True, scan_commits=1)
        self.assertTrue(hl.source_coverage_gate(self.status(degraded), degraded))
        rubber=self.healthy_streaming(source_healthy=False, live_state_confirmed=0,
            engine_live_state_confirmed=0, durable_baselines=373, hydrated_source_count=373,
            coverage_reconciliation_wallets_pending=2,
            source_degraded_coverage_eligible=True, scan_commits=1)
        self.assertFalse(hl.source_coverage_gate(self.status(rubber), rubber))

    def test_gap_counters_are_observable_for_acceptance(self):
        streaming=self.healthy_streaming(gaps=7, source_history_gapped=3,
            source_history_catching_up=2)
        rendered=hl.render_status({'stale':False,'age_seconds':4,'pause_requested':False,
            'status':{**self.status(streaming),'healthy':True,'execution_state':'NORMAL',
                'reconciliation':{'verified_through_unix_ms':int(time.time()*1000),
                    'recovery_pending':False}}})
        self.assertIn('stream gaps=7', rendered)


if __name__ == '__main__':
    unittest.main()
