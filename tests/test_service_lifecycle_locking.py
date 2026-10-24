"""Isolated regression tests for PBGui service lifecycle serialization."""

from __future__ import annotations

import subprocess
import json
import os
import ctypes
import sys
import threading
import time
from pathlib import Path

import pytest

import api.services as services
import PBApiServer
from PBCluster import PBCluster
from process_identity import ExactProcessSignalError
from process_identity import ProcessIdentity, write_process_identity
from service_lifecycle_lock import ServiceLifecycleBusyError, acquire_service_lifecycle_lock
import pbcoindata_process as coindata_process
from test_pbcoindata_process import retained_zombie, manual_view, offline_manager

_NATIVE_PROCESS = coindata_process.psutil.Process
_NATIVE_SLEEP = time.sleep
_REAL_SNAPSHOT = coindata_process.snapshot
_REAL_GUARD_READER = coindata_process.read_systemd_guard
_NATIVE_FSTATFS = ctypes.CDLL(None).fstatfs
_NATIVE_FSTATFS.argtypes = [ctypes.c_int, ctypes.c_void_p]
_NATIVE_FSTATFS.restype = ctypes.c_int


@pytest.fixture
def guard_barriers(manual_view, monkeypatch):
    """Real child/flock/publications; controlled namespace attachment and unit/cgroup metadata only."""
    from types import SimpleNamespace
    import select
    import pbcoindata_manager as manager
    state = manual_view
    state.child, state.stage, state.invocation = None, 'stopped', '0'*32
    state.active = 'inactive'
    state.commands, state.phases, state.children = [], [], []
    state.proc_device = (os.major(Path('/proc').stat().st_dev), os.minor(Path('/proc').stat().st_dev))
    table = state.data['/proc/self/mountinfo'].replace('0:2 / /proc', f'{state.proc_device[0]}:{state.proc_device[1]} / /proc')
    state.data['/proc/self/mountinfo'] = state.data['/proc/1/mountinfo'] = table
    original_view = manager._manual_start_view
    def view(root, deadline):
        """Reset only fixture counters between independent fully closed mount observations."""
        if not state.fds: state.opens.clear()
        return original_view(root,deadline)
    monkeypatch.setattr(manager,'_manual_start_view',view)
    original_library = manager.ctypes.CDLL
    synthetic = original_library(None).fstatfs
    def fstatfs(fd, output):
        """Use the actual syscall for actual /proc/locks descriptors."""
        return synthetic(fd,output) if fd in state.fds else _NATIVE_FSTATFS(fd,output)
    monkeypatch.setattr(manager.ctypes,'CDLL',lambda *a,**k: SimpleNamespace(fstatfs=fstatfs))
    monkeypatch.setattr(coindata_process.psutil,'Process',_NATIVE_PROCESS)
    monkeypatch.setattr(coindata_process,'snapshot',_REAL_SNAPSHOT)
    metadata_lock = threading.RLock()
    def read_guard(root, deadline):
        """Serialize only the shared synthetic FD metadata map, never the real daemon's guard."""
        with metadata_lock:
            return _REAL_GUARD_READER(root,deadline)
    monkeypatch.setattr(coindata_process,'read_systemd_guard',read_guard)
    monkeypatch.setattr(coindata_process,'manager_state',lambda *a:{'manager':'systemd','reason':'isolated unit'})
    repo = Path(__file__).resolve().parents[1]
    script = '''import os, sys, fcntl
sys.path.insert(0, REPO)
import pbcoindata_guard as guard
def barrier(name):
    print(name, flush=True)
    if not sys.stdin.readline(): raise SystemExit(0)
barrier('before-lock')
flock, truncate, write = fcntl.flock, os.ftruncate, os.write
def held(fd, flags):
    flock(fd, flags)
    barrier('flock-before-truncate')
def empty(fd, count):
    truncate(fd,count)
    barrier('empty-payload')
def partial(fd, data):
    cut = len(data)//2
    write(fd,data[:cut])
    barrier('partial-payload')
    return cut+write(fd,data[cut:])
fcntl.flock, os.ftruncate, os.write = held, empty, partial
class BarrierLease(guard.FileLease):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        fcntl.flock, os.ftruncate, os.write = flock, truncate, write
        barrier('complete-before-starting')
guard.FileLease = BarrierLease
lease = guard.acquire_daemon(os.getcwd())
barrier('starting')
import pbcoindata_process as process
process.register_current(os.getcwd())
barrier('ready')
'''.replace('REPO',repr(str(repo)))
    (state.root/'PBCoinData.py').write_text(script)
    for name in ('pbcoindata_guard.py','secure_files.py'):
        (state.root/name).write_text('# isolated entry marker\n')
    group = '/user.slice/test.slice/'+coindata_process.UNIT
    def stage():
        """A pipe barrier bounds every child operation without sleeps or external data."""
        assert select.select([state.child.stdout],[],[],5)[0], 'child barrier timed out'
        value = state.child.stdout.readline().strip()
        assert value, state.child.stderr.read() if state.child.poll() is not None else 'child exited'
        state.stage = value
        return value
    def advance():
        """Release precisely one isolated child barrier."""
        state.child.stdin.write('\n'); state.child.stdin.flush()
        return stage()
    def query(unit, deadline, **kwargs):
        """Keep unit and cgroup projection explicit; all process and guard checks remain real."""
        manager._check(deadline)
        pid = state.child.pid if state.child is not None and state.active=='active' else 0
        return {'Id':coindata_process.UNIT,'LoadState':'loaded','ActiveState':state.active,
                'SubState':'running' if pid else 'dead','MainPID':str(pid),'ControlGroup':group if pid else '',
                'Job':'','Result':'success','InvocationID':state.invocation,'WorkingDirectory':str(state.root),
                'StartLimitBurst':'5','StartLimitIntervalUSec':'10s',
                'ExecStart':'{ path='+sys.executable+' ; argv[]='+sys.executable+' -u '+str(state.root/'PBCoinData.py')+' ; ignore_errors=no ; }'}
    def command(action):
        """Only this fixture's own temporary child is started or stopped."""
        state.commands.append(action)
        if action in {'start','enable'}:
            state.invocation = f'{len(state.commands):032x}'
            state.child = subprocess.Popen([sys.executable,'-u',str(state.root/'PBCoinData.py')],cwd=state.root,
                stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            state.children.append(state.child)
            state.active = 'active'
            stage()
        else:
            state.child.stdin.close()
            # Own child remains unreaped until fixture teardown: positive zombie evidence.
            limit = time.monotonic()+5
            while _NATIVE_PROCESS(state.child.pid).status() != coindata_process.psutil.STATUS_ZOMBIE:
                assert time.monotonic()<limit
                threading.Event().wait(.005)
            state.active = 'inactive'
        return {'manager':'systemd'}
    monkeypatch.setattr(manager,'_query_unit',query)
    monkeypatch.setattr(manager,'_target_groups',lambda pid,deadline:group)
    monkeypatch.setattr(services,'PBGDIR',state.root)
    monkeypatch.setattr(services,'_optional_service_blocker',lambda *a:'')
    monkeypatch.setattr(services,'_systemd_enable_status',lambda *a,**k:{})
    monkeypatch.setattr(services,'_systemd_service_action',lambda name,action:command(action))
    state.command, state.advance = command, advance
    def readonly(monkeypatch_context):
        """Reject every attempted create, write, truncate or flock of the observed singleton."""
        import fcntl
        previous_open, previous_write, previous_truncate = os.open, os.write, os.ftruncate
        def opened(path,flags,*args,**kwargs):
            """Only O_RDONLY is permissible for the singleton observer."""
            if Path(path).name=='pbcoindata-singleton.lock':
                assert not flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC)
            return previous_open(path,flags,*args,**kwargs)
        def writing(fd,*args):
            """A public unrelated descriptor is not the singleton under test."""
            assert os.readlink(f'/proc/self/fd/{fd}')!=str(state.root/'data/locks/pbcoindata-singleton.lock')
            return previous_write(fd,*args)
        def truncating(fd,*args):
            """No in-place truncation may be hidden by restoring the same bytes."""
            assert os.readlink(f'/proc/self/fd/{fd}')!=str(state.root/'data/locks/pbcoindata-singleton.lock')
            return previous_truncate(fd,*args)
        monkeypatch_context.setattr(os,'open',opened)
        monkeypatch_context.setattr(os,'write',writing)
        monkeypatch_context.setattr(os,'ftruncate',truncating)
        monkeypatch_context.setattr(fcntl,'flock',lambda *a:pytest.fail('Systemd observer acquired flock'))
    state.readonly=readonly
    try:
        yield state
    finally:
        monkeypatch.setattr(coindata_process.time,'sleep',_NATIVE_SLEEP)
        for child in state.children:
            try:
                if child.poll() is None:
                    child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
            finally:
                for stream in (child.stdin,child.stdout,child.stderr):
                    if stream is not None and not stream.closed: stream.close()
            assert child.returncode is not None
            assert all(stream.closed for stream in (child.stdin,child.stdout,child.stderr))
        assert not state.fds


class TestReadOnlySystemdGuard:
    """Real early daemon phases never let a status observer steal or rewrite its guard."""

    @pytest.mark.parametrize('entry',['api','starter'])
    def test_restart_accepts_scheduler_state_changes(self,guard_barriers,monkeypatch,capsys,entry):
        """Real restart retires the old lifetime and reaches a new ready one across R/S samples."""
        import starter
        guard=coindata_process.guard_protocol
        state=guard_barriers
        state.command('start')
        while state.stage!='ready':state.advance()
        old_child=state.child
        old_identity=guard.process_observation(old_child.pid)
        original=guard.process_observation
        samples={}
        terminals=[]
        def observing(pid='self'):
            """Alter only live scheduler states of our own children, preserving real identities/exits."""
            observed=original(pid)
            if any(child.pid==pid for child in state.children):
                if observed[1] in {'Z','X'}:
                    terminals.append((pid,observed))
                else:
                    values=samples.setdefault(pid,[])
                    observed=(observed[0],'R' if len(values)%2==0 else 'S',observed[2])
                    values.append(observed)
            return observed
        def tick(delay):
            """Advance one bounded pipe barrier while the real readiness loop runs."""
            assert state.child is not old_child
            assert state.stage!='ready'
            state.phases.append(state.stage)
            state.advance()
        monkeypatch.setattr(guard,'process_observation',observing)
        monkeypatch.setattr(coindata_process.time,'sleep',tick)
        if entry=='api':
            try:
                result=services._service_action('pbcoindata','restart')
            except services.HTTPException as exc:
                pytest.fail(str(exc.detail),pytrace=False)
        else:
            monkeypatch.chdir(state.root)
            monkeypatch.setattr(sys,'argv',['starter.py','--restart','PBCoinData'])
            monkeypatch.setattr(starter,'_systemd_action',lambda name,act:(bool(state.command(act)),True))
            try:
                starter.main()
            except SystemExit as exc:
                pytest.fail(f'Starter exit {exc.code}: {capsys.readouterr().err}',pytrace=False)
            result=coindata_process.systemd_observation(state.root)
        assert result['running'] and result['actual_status']=='running'
        assert state.commands==['start','stop','start']
        assert state.stage=='ready'
        assert state.phases==['before-lock','flock-before-truncate','empty-payload','partial-payload','complete-before-starting','starting']
        assert original(old_child.pid)[1] in {'Z','X'}
        assert any(pid==old_child.pid and obs[0]==old_identity[0] and obs[2]==old_identity[2]
                   for pid,obs in terminals)
        new_identity=original(state.child.pid)
        assert state.child.pid!=old_child.pid
        registration=guard.read_publications(state.root)['records']
        assert any(record.get('phase')=='ready' and record['pid']==state.child.pid
                   and abs(record['create_time']-new_identity[0])<.01 for record in registration)
        for pid in (old_child.pid,state.child.pid):
            values=samples[pid]
            assert len(values)>=2 and {value[1] for value in values}=={'R','S'}
            assert len({(value[0],value[2]) for value in values})==1

    @pytest.mark.parametrize('entry,action',[('api','start'),('api','enable'),('api','restart'),('starter','start'),('starter','restart')])
    def test_real_barriers_reach_ready_without_observer_flock(self, guard_barriers, monkeypatch, entry, action):
        """The actual API observes every early phase and only succeeds at real ready."""
        import starter
        state = guard_barriers
        if action=='restart':
            state.command('start')
            while state.stage!='ready': state.advance()
        native_sleep = coindata_process.time.sleep
        def tick(delay):
            """Repeated observations, including API status, cannot advance or modify the child."""
            with monkeypatch.context() as observing:
                state.readonly(observing)
                path = state.root/'data/locks/pbcoindata-singleton.lock'
                before = path.read_bytes() if path.exists() else None
                status = services._service_status('pbcoindata')
                assert status['actual_status']=='starting', status
                assert not status['running']
                assert (path.read_bytes() if path.exists() else None)==before
                state.phases.append(state.stage)
            state.advance()
        monkeypatch.setattr(coindata_process.time,'sleep',tick)
        try:
            if entry=='api':
                result = services._service_action('pbcoindata',action)
            else:
                monkeypatch.chdir(state.root)
                monkeypatch.setattr(sys,'argv',['starter.py','--'+action,'PBCoinData'])
                monkeypatch.setattr(starter,'_systemd_action',lambda name,act:(bool(state.command(act)),True))
                starter.main()
                result=coindata_process.systemd_observation(state.root)
        except services.HTTPException as exc:
            pytest.fail(f'Barrier {state.stage}: {exc.detail}', pytrace=False)
        except BaseException as exc:
            pytest.fail(f'Barrier {state.stage}: {type(exc).__name__}: {str(exc)[:600]}', pytrace=False)
        assert result['running'] and state.stage=='ready'
        assert state.phases==['before-lock','flock-before-truncate','empty-payload','partial-payload','complete-before-starting','starting']
        monkeypatch.setattr(coindata_process.time,'sleep',native_sleep)
        assert coindata_process.systemd_start_preflight(state.root)['running']
        stopped = services._service_action('pbcoindata','stop')
        assert not stopped['running'] and stopped['actual_status']=='stopped'

    def test_unbound_and_expired_awaiting_are_unknown(self, guard_barriers):
        """No full publication and no current attempt means no readiness or Stop rights."""
        state = guard_barriers
        state.command('start')
        assert coindata_process.systemd_observation(state.root)['actual_status']=='unknown'
        with coindata_process.manual_action(state.root,'start') as auth:
            context = coindata_process.SystemdAttempt(auth,time.monotonic()-1,{},True)
            assert coindata_process.systemd_observation(state.root,context=context)['actual_status']=='unknown'

    def test_parallel_status_queries_preserve_early_guard(self, guard_barriers, monkeypatch):
        """Concurrent status callers share only observation; neither grants Stop nor rewrites bytes."""
        from concurrent.futures import ThreadPoolExecutor
        state = guard_barriers
        with coindata_process.manual_action(state.root,'start') as auth:
            before = coindata_process.systemd_start_preflight(state.root)
            state.command('start')
            context = coindata_process.SystemdAttempt(auth,time.monotonic()+10,before,True)
            monkeypatch.setattr(coindata_process,'_SYSTEMD_ATTEMPTS',{state.root:context})
            for phase in ('before-lock','flock-before-truncate','empty-payload','partial-payload','complete-before-starting','starting'):
                assert state.stage == phase
                path = state.root/'data/locks/pbcoindata-singleton.lock'
                previous = path.read_bytes() if path.exists() else None
                with monkeypatch.context() as observed:
                    state.readonly(observed)
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        results = list(executor.map(lambda _:services._service_status('pbcoindata'),range(2)))
                    assert all(r['actual_status']=='starting' and not r['running'] for r in results), results
                    if phase != 'starting': assert not any(r['can_stop'] for r in results)
                    assert (path.read_bytes() if path.exists() else None)==previous
                state.advance()
            assert coindata_process.systemd_observation(state.root,context=context)['running']
            context.active=False

    def test_legacy_probe_is_preserved_but_attempt_can_await(self,guard_barriers):
        """The old conservative primitive stays Unknown; the typed attempt awaits and reaches Ready."""
        state=guard_barriers
        with coindata_process.manual_action(state.root,'start') as auth:
            before=coindata_process.systemd_start_preflight(state.root)
            state.command('start');state.advance()
            context=coindata_process.SystemdAttempt(auth,time.monotonic()+10,before,True)
            legacy=_REAL_SNAPSHOT(state.root)
            assert legacy['status']=='unknown' and 'singleton transition is unconfirmed' in legacy['error']
            assert coindata_process.systemd_observation(state.root,context=context)['actual_status']=='starting'
            while state.stage!='ready':state.advance()
            assert coindata_process.systemd_observation(state.root,context=context)['running']

    @pytest.mark.parametrize('fault',['invocation','birth','payload','journal','foreign-publication','inode'])
    def test_bound_conflicts_never_become_awaiting(self,guard_barriers,monkeypatch,fault):
        """A concrete conflict is not hidden by the authorized startup window."""
        state = guard_barriers
        with coindata_process.manual_action(state.root,'start') as auth:
            before = coindata_process.systemd_start_preflight(state.root)
            state.command('start')
            context = coindata_process.SystemdAttempt(auth,time.monotonic()+10,before,True)
            first = coindata_process.systemd_observation(state.root,context=context)
            assert first['actual_status']=='starting'
            if fault=='invocation': state.invocation='f'*32
            elif fault=='birth': context.identity=ProcessIdentity(state.child.pid,context.identity.create_time+1)
            elif fault=='payload':
                state.advance()  # Real granted lock, with contradictory complete bytes.
                (state.root/'data/locks/pbcoindata-singleton.lock').write_text('{"owner":"foreign"}')
            elif fault=='inode':
                state.advance()
                context.guard_inode=(1,1)
            elif fault=='foreign-publication':
                registration = coindata_process.guard_protocol.registration_path(state.root)
                registration.parent.mkdir(parents=True,exist_ok=True)
                registration.write_text(json.dumps({**coindata_process.guard_protocol.scope(state.root),
                    'pid':os.getpid(),'create_time':_NATIVE_PROCESS(os.getpid()).create_time(),'phase':'starting'}))
            else:
                journal=coindata_process.guard_protocol.journal_path(state.root)
                journal.parent.mkdir(parents=True,exist_ok=True); journal.write_text('{')
            result=coindata_process.systemd_observation(state.root,context=context)
            assert result['actual_status']=='unknown' and result['error'] and not result['can_stop'],result

    @pytest.mark.parametrize('fault',['fd-mount','fake-procfs','ioctl','mount-denied'])
    def test_lock_reader_requires_complete_namespace_and_procfs(self,guard_barriers,fault):
        """Denied, mismatched and non-procfs evidence closes all FDs and cannot prove absence."""
        state=guard_barriers
        state.fd_fault=fault
        count=len(os.listdir('/proc/self/fd'))
        with pytest.raises((coindata_process.ProcessStateError,OSError,ValueError)):
            coindata_process.read_systemd_guard(state.root,time.monotonic()+5)
        assert not state.fds and len(os.listdir('/proc/self/fd'))==count

    def test_open_fd_alone_does_not_establish_guard_ownership(self,guard_barriers):
        """Opening a regular singleton file is not a granted FLOCK or ready proof."""
        state=guard_barriers
        path=state.root/'data/locks/pbcoindata-singleton.lock'
        path.parent.mkdir(parents=True)
        path.write_bytes(b'')
        with path.open('rb'):
            observation=coindata_process.read_systemd_guard(state.root,time.monotonic()+5)
            assert observation.holder is None

    @pytest.mark.parametrize('fault',['denied','truncated','oversized','deadline'])
    def test_lock_listing_read_failures_close_descriptors(self,guard_barriers,monkeypatch,fault):
        """No incomplete listing or exhausted budget is accepted as a missing owner."""
        state=guard_barriers
        original_read=os.read
        reads=[0]
        def read(fd,count):
            """Fault only the explicitly identified public /proc/locks descriptor."""
            if fd not in state.fds and os.readlink(f'/proc/self/fd/{fd}')=='/proc/locks':
                reads[0]+=1
                if fault=='denied': raise PermissionError('isolated listing denial')
                if fault=='truncated': return b'1: FLOCK' if reads[0]==1 else b''
                if fault=='oversized': return b'x'*count
            return original_read(fd,count)
        monkeypatch.setattr(os,'read',read)
        count=len(os.listdir('/proc/self/fd'))
        with pytest.raises((coindata_process.ProcessStateError,OSError,ValueError,TimeoutError)):
            coindata_process.read_systemd_guard(state.root,time.monotonic()+(-1 if fault=='deadline' else 5))
        assert not state.fds and len(os.listdir('/proc/self/fd'))==count

    @pytest.mark.parametrize('fault',['replacement','missing'])
    def test_real_inode_replacement_is_refused(self,guard_barriers,fault):
        """The attempt retains the granted original inode even before payload publication."""
        state=guard_barriers
        with coindata_process.manual_action(state.root,'start') as auth:
            before=coindata_process.systemd_start_preflight(state.root)
            state.command('start');state.advance()
            context=coindata_process.SystemdAttempt(auth,time.monotonic()+10,before,True)
            assert coindata_process.systemd_observation(state.root,context=context)['actual_status']=='starting'
            path=state.root/'data/locks/pbcoindata-singleton.lock'
            if fault=='replacement':
                replacement=path.with_suffix('.replacement')
                replacement.write_bytes(b'');replacement.replace(path)
            else:path.unlink()
            result=coindata_process.systemd_observation(state.root,context=context)
            if result['actual_status']!='unknown' or 'inode changed' not in result['error']:
                pytest.fail(f"Changed guard path ({fault}): {result['actual_status']}",pytrace=False)

    def test_stop_reobserves_unit_exit_and_retained_publications(self,guard_barriers,monkeypatch):
        """MainPID and Invocation clearing during a sample require exact exit before success."""
        import pbcoindata_manager as manager
        state=guard_barriers
        state.command('start')
        while state.stage!='ready':state.advance()
        query=manager._query_unit
        calls=[0]
        def stop_command(name,action):
            """Delay only this child's real exit until the observer has read its live unit."""
            assert action=='stop'
            calls[0]=1
            return {'manager':'systemd'}
        def advancing_query(*args,**kwargs):
            """End the actual lifetime between the original and fresh unit samples."""
            if calls[0]:
                calls[0]+=1
                if calls[0]==3:
                    state.command('stop');state.invocation='0'*32
            return query(*args,**kwargs)
        monkeypatch.setattr(services,'_systemd_service_action',stop_command)
        monkeypatch.setattr(manager,'_query_unit',advancing_query)
        result=services._service_action('pbcoindata','stop')
        assert result['actual_status']=='stopped' and not result['running']
        assert coindata_process.guard_protocol.registration_path(state.root).exists()

    def test_mixed_publication_sample_is_bounded_progress(self,guard_barriers,monkeypatch):
        """A same-invocation publication change is reread; a stable conflict is never ready."""
        state=guard_barriers
        with coindata_process.manual_action(state.root,'start') as auth:
            before=coindata_process.systemd_start_preflight(state.root)
            state.command('start')
            context=coindata_process.SystemdAttempt(auth,time.monotonic()+10,before,True)
            while state.stage!='complete-before-starting': state.advance()
            original=coindata_process._publication_bytes
            once=[False]
            def publish_between_reads(root):
                """Advance actual starting publication after capturing the previous original bytes."""
                value=original(root)
                if not once[0]:
                    once[0]=True; state.advance()
                return value
            monkeypatch.setattr(coindata_process,'_publication_bytes',publish_between_reads)
            result=coindata_process.systemd_observation(state.root,context=context)
            assert result['actual_status']=='starting' and result['_phase']==coindata_process.Awaiting.SAMPLE
            state.advance()
            assert coindata_process.systemd_observation(state.root,context=context)['running']

    def test_mainpid_cannot_disappear_after_binding(self,guard_barriers,monkeypatch):
        """A pre-MainPID unit phase is valid; losing an already bound MainPID is a conflict."""
        import pbcoindata_manager as manager
        state=guard_barriers
        with coindata_process.manual_action(state.root,'start') as auth:
            before=coindata_process.systemd_start_preflight(state.root)
            state.command('start')
            context=coindata_process.SystemdAttempt(auth,time.monotonic()+10,before,True)
            query=manager._query_unit
            def no_mainpid(*args,**kwargs):
                """Control only the unit projection, keeping the real child before guard acquisition."""
                return query(*args,**kwargs)|{'MainPID':'0','ControlGroup':'','ActiveState':'activating','SubState':'start'}
            monkeypatch.setattr(manager,'_query_unit',no_mainpid)
            initial=coindata_process.systemd_observation(state.root,context=context)
            assert initial['actual_status']=='starting' and context.identity is None
            monkeypatch.setattr(manager,'_query_unit',query)
            assert coindata_process.systemd_observation(state.root,context=context)['actual_status']=='starting'
            assert context.identity.pid==state.child.pid
            monkeypatch.setattr(manager,'_query_unit',no_mainpid)
            lost=coindata_process.systemd_observation(state.root,context=context)
            if lost['actual_status']!='unknown' or lost['can_stop']:
                pytest.fail(f"Lost bound MainPID: actual={lost['actual_status']}, can_stop={lost['can_stop']}",pytrace=False)

    def test_parallel_mainpid_progress_is_one_coherent_attempt(self,guard_barriers,monkeypatch):
        """A status caller cannot bind a newer PID through another caller's older unit sample."""
        from concurrent.futures import ThreadPoolExecutor
        import pbcoindata_manager as manager
        state=guard_barriers
        with coindata_process.manual_action(state.root,'start') as auth:
            before=coindata_process.systemd_start_preflight(state.root)
            state.command('start')
            context=coindata_process.SystemdAttempt(auth,time.monotonic()+10,before,True)
            query,reader=manager._query_unit,coindata_process.read_systemd_guard
            sampled,release=threading.Event(),threading.Event()
            first_query,first_read=[True],[True]
            def query_progress(*args,**kwargs):
                """The unit advances from pre-MainPID to the real child, within one InvocationID."""
                value=query(*args,**kwargs)
                if first_query[0]:
                    first_query[0]=False
                    return value|{'MainPID':'0','ControlGroup':'','ActiveState':'activating','SubState':'start'}
                return value
            def paused_reader(*args,**kwargs):
                """Hold the older whole sample, rather than freezing any real flock operation."""
                if first_read[0]:
                    first_read[0]=False;sampled.set()
                    assert release.wait(5)
                return reader(*args,**kwargs)
            monkeypatch.setattr(manager,'_query_unit',query_progress)
            monkeypatch.setattr(coindata_process,'read_systemd_guard',paused_reader)
            with ThreadPoolExecutor(max_workers=2) as executor:
                first=executor.submit(coindata_process.systemd_observation,state.root,context=context)
                assert sampled.wait(5)
                acquired=context.lock.acquire(blocking=False)
                if acquired:context.lock.release()
                second=executor.submit(coindata_process.systemd_observation,state.root,context=context)
                release.set()
                results=[first.result(timeout=5),second.result(timeout=5)]
            if acquired:
                pytest.fail('Concurrent callers can mutate bindings through an older unit sample',pytrace=False)
            assert all(result['actual_status']=='starting' for result in results),results

    def test_guard_replacement_between_final_samples_is_hard_conflict(self,guard_barriers,monkeypatch):
        """A known inode change is never classified as publication-writing progress."""
        state=guard_barriers
        with coindata_process.manual_action(state.root,'start') as auth:
            before=coindata_process.systemd_start_preflight(state.root)
            state.command('start');state.advance()
            context=coindata_process.SystemdAttempt(auth,time.monotonic()+10,before,True)
            reader=coindata_process.read_systemd_guard
            calls=[0]
            def replace_before_fresh_read(*args,**kwargs):
                """Replace only the isolated pathname while the child's original FD remains held."""
                calls[0]+=1
                if calls[0]==2:
                    path=state.root/'data/locks/pbcoindata-singleton.lock'
                    replacement=path.with_suffix('.new')
                    replacement.write_bytes(b'');replacement.replace(path)
                return reader(*args,**kwargs)
            monkeypatch.setattr(coindata_process,'read_systemd_guard',replace_before_fresh_read)
            result=coindata_process.systemd_observation(state.root,context=context)
            if result['actual_status']!='unknown':
                pytest.fail(f"Changed final inode treated as {result['actual_status']}",pytrace=False)

    @pytest.mark.parametrize('suffix,valid',[('',True),(' -> FLOCK ADVISORY WRITE 43 00:01:9 0 EOF',True)])
    def test_granted_and_waiting_kernel_records(self,suffix,valid):
        """Only the granted owner counts, even when another process waits for its lock."""
        text = '1: FLOCK ADVISORY WRITE 42 00:01:9 0 EOF\n'
        if suffix: text += '1:'+suffix+'\n'
        assert coindata_process._lock_holder(text,(os.makedev(0,1),9))==42

    @pytest.mark.parametrize('record',[
        '1: -> FLOCK ADVISORY WRITE 42 00:01:9 0 EOF\n',
        '1: POSIX ADVISORY WRITE 42 00:01:9 0 EOF\n',
        '1: FLOCK ADVISORY READ 42 00:01:9 0 EOF\n',
        '1: FLOCK ADVISORY WRITE 42 00:01:9 0 EOF', 'broken\n',
        '1: FLOCK ADVISORY WRITE 42 00:01:9 0 EOF\n'*32769],
        ids=['waiter-only','posix','read-lock','truncated','malformed','oversized'])
    def test_ambiguous_kernel_records_fail_closed(self,record):
        """Waiting alone, wrong types, truncation and oversize never prove a free guard."""
        with pytest.raises(coindata_process.ProcessStateError):
            coindata_process._lock_holder(record,(os.makedev(0,1),9))


@pytest.fixture
def systemd_readiness(tmp_path, monkeypatch):
    """Test action wiring and deadlines with explicit synthetic kernel/readiness boundaries."""
    from types import SimpleNamespace
    import psutil
    import pbcoindata_manager as manager
    import starter
    state = SimpleNamespace(root=tmp_path, clock=0.0, pid=0, ready=False, active='inactive',
                            mode='ready', commands=[], budgets=[], processes={}, fault=None, invocation='0'*32)
    native_process = psutil.Process
    for name in ('PBCoinData.py', 'pbcoindata_guard.py', 'secure_files.py'):
        (tmp_path / name).write_text('# isolated guarded entry\n')
    group = '/user.slice/user-1000.slice/user@1000.service/app.slice/' + coindata_process.UNIT
    original_snapshot = coindata_process.snapshot
    def identity():
        """Keep the current lifetime separate from numeric PID existence."""
        return ProcessIdentity(state.pid, state.processes[state.pid]) if state.pid else None
    def observe(root, **kwargs):
        """Only ready data bypasses the real absent-publication singleton probe."""
        if state.ready and state.pid:
            current = ProcessIdentity(state.pid + 1, 1) if state.fault == 'snapshot' else identity()
            return {'status': 'running', 'pid': state.pid, 'identity': current}
        return original_snapshot(root, **kwargs)
    def guard_sample(root, deadline):
        """Supply typed kernel evidence; separate barrier tests exercise the real reader."""
        path = root / 'data/locks/pbcoindata-singleton.lock'
        if path.exists():
            info = path.stat()
            owner = coindata_process._lock_holder(Path('/proc/locks').read_text(), (info.st_dev,info.st_ino))
            if owner is not None:
                return coindata_process.GuardObservation(root,(info.st_dev,info.st_ino),path.read_bytes(),
                    ProcessIdentity(owner,native_process(owner).create_time()))
        current = identity()
        payload = json.dumps({'owner':'daemon','protocol':coindata_process.guard_protocol.PROTOCOL,
                              'pid':current.pid,'create_time':current.create_time}).encode() if current else b''
        return coindata_process.GuardObservation(root,(1,2 if state.ready and state.mode=='inode' else 1),payload,current)
    def inspect(root, pid, expected=None):
        """Reject reuse and preserve positive exit evidence without real processes."""
        if pid not in state.processes:
            return {'status': 'stopped', 'pid': None}
        current = ProcessIdentity(pid, state.processes[pid])
        if expected is not None and expected != current:
            return {'status': 'unknown', 'pid': pid}
        return {'status': 'running', 'pid': pid, 'identity': current}
    class Process:
        """Minimal psutil boundary for an explicitly registered fake kernel lifetime."""
        def __init__(self, pid):
            if pid not in state.processes: raise psutil.NoSuchProcess(pid)
            self.pid = pid
        def create_time(self):
            """Return the fake kernel birth time."""
            return state.processes[self.pid]
        def exe(self):
            """The effective interpreter is a real public executable."""
            return sys.executable
        def status(self):
            """An existing synthetic lifetime is executing."""
            return psutil.STATUS_RUNNING
    def run(argv, **kwargs):
        """Exercise the original unit-property parser and its shrinking timeout."""
        assert argv[:3] == ['systemctl', '--user', 'show']
        state.budgets.append(kwargs['timeout'])
        assert 0 < kwargs['timeout'] <= min(3, max(.001, 10-state.clock))
        value = {'Id': coindata_process.UNIT, 'LoadState': 'loaded', 'ActiveState': state.active,
                 'SubState': 'running' if state.active == 'active' else 'dead', 'MainPID': str(state.pid),
                 'ControlGroup': group if state.pid else '', 'Job': '', 'Result': 'success', 'InvocationID':state.invocation,
                 'WorkingDirectory': str(tmp_path), 'StartLimitBurst': '5', 'StartLimitIntervalUSec': '10s',
                 'ExecStart': '{ path=' + sys.executable + ' ; argv[]=' + sys.executable + ' -u ' + str(tmp_path / 'PBCoinData.py') + ' ; ignore_errors=no ; }'}
        if state.fault == 'unit': value['Id'] = 'other.service'
        if state.fault == 'cgroup': value['ControlGroup'] = group + '/foreign.service'
        if state.fault == 'entry': value['WorkingDirectory'] = str(tmp_path / 'other')
        if state.fault == 'denied': raise PermissionError('isolated unit access denied')
        return subprocess.CompletedProcess(argv, 0, '\n'.join(k+'='+v for k,v in value.items())+'\n', '')
    def command(action):
        """Model a successful systemctl result independently of later daemon readiness."""
        state.commands.append(action)
        if action in {'stop','disable'}:
            state.processes.pop(state.pid, None)
            state.pid, state.ready, state.active = 0, False, 'inactive'
            if state.mode == 'later-obstacle':
                coindata_process._write_launch(tmp_path, {'version':1,'token':'a'*32,'pid':None,'create_time':None})
        else:
            state.invocation = f'{len(state.commands):032x}'
            state.pid = max([22, *state.processes]) + 1
            state.processes[state.pid] = 100 + state.pid
            state.active, state.ready = 'active', state.mode == 'ready'
        return {'running': True, 'manager': 'systemd'}
    def tick(delay):
        """Advance only the isolated clock, applying a delayed valid or failed initialization."""
        state.clock += delay
        if state.mode in {'delayed','inode'} and state.clock >= .1: state.ready = True
        if state.mode == 'failed':
            state.processes.pop(state.pid, None)
            state.pid, state.active = 0, 'failed'
        if state.mode in {'unit','cgroup','guard','reuse','snapshot','entry','denied'}:
            state.ready = True
            if state.mode == 'reuse': state.processes[state.pid] += 1
            else: state.fault = state.mode
    monkeypatch.setattr(manager.subprocess, 'run', run)
    monkeypatch.setattr(manager, '_target_groups', lambda pid, deadline: group)
    monkeypatch.setattr(coindata_process, 'manager_state', lambda *a: {'manager':'systemd','reason':'isolated installed unit'})
    monkeypatch.setattr(coindata_process, 'snapshot', observe)
    monkeypatch.setattr(coindata_process, 'read_systemd_guard', guard_sample)
    monkeypatch.setattr(coindata_process, 'inspect_pid', inspect)
    monkeypatch.setattr(coindata_process.psutil, 'Process', Process)
    monkeypatch.setattr(coindata_process, 'registration_ready', lambda root, current, **kwargs: state.ready and current == identity() and state.fault != 'guard')
    monkeypatch.setattr(coindata_process.time, 'monotonic', lambda: state.clock)
    monkeypatch.setattr(coindata_process.time, 'sleep', tick)
    monkeypatch.setattr(services, 'PBGDIR', tmp_path)
    monkeypatch.setattr(services, '_optional_service_blocker', lambda *a: '')
    monkeypatch.setattr(services, '_systemd_enable_status', lambda *a,**k: {'enabled':False,'can_enable':True})
    monkeypatch.setattr(services, '_systemd_service_action', lambda name, action: command(action))
    monkeypatch.setattr(starter, '_systemd_action', lambda name, action: (bool(command(action)),True) if name == 'PBCoinData' else (True,True))
    state.command, state.identity = command, identity
    return state


@pytest.mark.parametrize('action', ['start','enable'])
def test_systemd_active_without_guard_is_not_start_success(systemd_readiness, action):
    """An active unit which fails before guard readiness must return a useful HTTP 409."""
    state = systemd_readiness
    state.mode = 'failed'
    with pytest.raises(services.HTTPException) as error:
        services._service_action('pbcoindata', action)
    assert error.value.status_code == 409
    assert error.value.detail == 'Bound PBCoinData MainPID disappeared during initialization'
    assert state.commands == [action] and state.clock < 10
    assert state.ready is False
    assert state.pid == 0
    assert services._service_status('pbcoindata')['actual_status'] == 'failed'


@pytest.mark.parametrize('action', ['start','enable','restart'])
def test_systemd_singleton_fence_precedes_any_command(systemd_readiness, action):
    """A real unrelated singleton blocks both start and an avoidable restart stop."""
    state = systemd_readiness
    held = coindata_process.FileLease(state.root, 'singleton', 'isolated-other-owner')
    try:
        with pytest.raises(services.HTTPException) as error:
            services._service_action('pbcoindata', action)
        assert error.value.status_code == 409 and state.commands == []
    finally:
        held.release()
    assert services._service_action('pbcoindata', 'start')['running']


@pytest.mark.parametrize('entry', ['api','starter'])
@pytest.mark.parametrize('mode', ['delayed','timeout','failed','unit','cgroup','guard','reuse','snapshot','entry','denied'])
def test_systemd_shared_readiness_is_bounded(systemd_readiness, monkeypatch, entry, mode):
    """Both entry points wait for true readiness and stop on failed or changed evidence."""
    import starter
    state = systemd_readiness
    state.mode = mode
    def invoke():
        """Use actual handlers and their original action locks."""
        if entry == 'api': return services._service_action('pbcoindata','start')
        monkeypatch.chdir(state.root)
        monkeypatch.setattr(sys,'argv',['starter.py','--start','PBCoinData','PBCluster'])
        return starter.main()
    if mode == 'delayed':
        invoke()
        assert state.ready and .1 <= state.clock < 10
    else:
        with pytest.raises(services.HTTPException if entry == 'api' else SystemExit): invoke()
        assert state.clock <= 10
    assert state.budgets and state.commands[0] == 'start'
    with coindata_process.actions(state.root,'after-test'):
        pass


def test_systemd_healthy_singleton_and_later_restart_fence(systemd_readiness):
    """The own healthy singleton is accepted; a new journal obstacle after stop blocks replacement."""
    state = systemd_readiness
    state.command('start')
    assert coindata_process.systemd_start_preflight(state.root)['running']
    state.mode = 'later-obstacle'
    with pytest.raises(services.HTTPException): services._service_action('pbcoindata','restart')
    assert state.commands == ['start','stop']
    assert not services._service_status('pbcoindata')['running']


@pytest.mark.parametrize('action', ['stop','disable'])
def test_systemd_background_start_status_and_safe_stop(systemd_readiness, action):
    """An unbound unready MainPID stays Unknown and cannot acquire Stop permission."""
    state = systemd_readiness
    state.mode = 'timeout'
    state.command('start')
    status = services._service_status('pbcoindata')
    assert status['actual_status'] == 'unknown' and not status['running'] and not status['can_stop']
    with pytest.raises(services.HTTPException): services._service_action('pbcoindata',action)
    assert state.commands == ['start']


def test_systemd_guard_inode_change_during_start_is_refused(systemd_readiness, monkeypatch):
    """A changed guard inode between positive initialization and ready cannot authorize success."""
    state = systemd_readiness
    state.mode = 'inode'
    with pytest.raises(services.HTTPException) as error:
        services._service_action('pbcoindata','start')
    assert error.value.status_code == 409 and 'guard inode changed' in error.value.detail


def test_systemd_unknown_does_not_authorize_stop(systemd_readiness):
    """running=false from an ownership conflict is never evidence that Stop completed."""
    state = systemd_readiness
    held = coindata_process.FileLease(state.root,'singleton','isolated-conflict')
    try:
        status = services._service_status('pbcoindata')
        assert status['actual_status'] == 'unknown' and not status['running']
        for action in ('stop','disable'):
            with pytest.raises(services.HTTPException): services._service_action('pbcoindata',action)
        assert not state.commands
    finally:
        held.release()


def test_systemd_start_returns_budget_bound_confirmation(systemd_readiness, monkeypatch):
    """API returns the fresh shared proof without adding another unbudgeted readiness scan."""
    state = systemd_readiness
    state.mode = 'delayed'
    monkeypatch.setattr(services, '_service_status', lambda *a: pytest.fail('No second readiness budget'))
    result = services._service_action('pbcoindata','start')
    assert result['running'] and result['actual_status'] == 'running'
    assert not any(key.startswith('_') for key in result)
    assert state.clock == .1


@pytest.mark.parametrize('manager_after_failure',['systemd','unknown'])
def test_systemd_failed_restart_diagnostic_is_read_only(systemd_readiness,monkeypatch,manager_after_failure):
    """Error reporting after failed replacement must never take a legacy singleton probe."""
    state=systemd_readiness
    state.command('start')
    state.mode='failed'
    monkeypatch.setattr(coindata_process,'manager_state',lambda root:{
        'manager':manager_after_failure if state.active=='failed' else 'systemd'})
    original=coindata_process.FileLease
    probes=[]
    def lease(root,name,owner):
        """Record only real temporary probe attempts, without touching external data."""
        if name=='singleton':probes.append(owner)
        return original(root,name,owner)
    monkeypatch.setattr(coindata_process,'FileLease',lease)
    with pytest.raises(services.HTTPException) as error:
        services._service_action('pbcoindata','restart')
    assert error.value.status_code==409
    assert probes==[],probes


@pytest.mark.parametrize('fault', ['corrupt','pending'])
def test_systemd_journal_fences_release_probe(systemd_readiness, fault):
    """Failed preflight retains journal bytes and closes every temporary singleton probe."""
    import os
    state = systemd_readiness
    path = coindata_process.guard_protocol.journal_path(state.root)
    path.parent.mkdir(parents=True, exist_ok=True)
    if fault == 'corrupt': path.write_text('{')
    else: coindata_process._write_launch(state.root, {'version':1,'token':'a'*32,'pid':None,'create_time':None})
    original, count = path.read_bytes(), len(os.listdir('/proc/self/fd'))
    with pytest.raises(services.HTTPException): services._service_action('pbcoindata','start')
    assert not state.commands and path.read_bytes() == original
    assert len(os.listdir('/proc/self/fd')) == count


@pytest.mark.parametrize('entry',['api','starter'])
def test_retained_zombie_service_sequence(retained_zombie,monkeypatch,entry):
    """Actual handlers see stopped despite ready/Z, then start and restart guarded replacements."""
    import signal
    import psutil
    import starter
    state=retained_zombie;signals=[];following=[]
    original=coindata_process.signal_exact_process
    def send(identity,signum):
        """Only the new executing daemon can ever be signalled."""
        assert identity.pid not in {state.identity.pid,state.parent.pid}
        signals.append((identity,signum));return original(identity,signum)
    monkeypatch.setattr(coindata_process,'signal_exact_process',send)
    monkeypatch.setattr(services,'PBGDIR',state.root)
    monkeypatch.setattr(services,'_optional_service_blocker',lambda *a:'')
    monkeypatch.setattr(services,'_systemd_enable_status',lambda *a,**k:{})
    monkeypatch.setattr(starter,'_systemd_action',lambda service,action:(following.append((service,action)) or True,True))
    monkeypatch.chdir(state.root)
    first=None
    for action in ('stop','start','restart'):
        if entry=='api':
            result=getattr(services,action+'_service')('pbcoindata',session=None)
            assert result['actual_status']==('stopped' if action=='stop' else 'running')
            assert result['running']==(action!='stop') and not result['error']
        else:
            monkeypatch.setattr(sys,'argv',['starter.py','--'+action,'PBCoinData','PBCluster'])
            starter.main()
        current=coindata_process.snapshot(state.root)
        assert current['status']==('stopped' if action=='stop' else 'running')
        if action=='start':first=current['identity']
        if action=='restart':assert current['identity']!=first
        assert psutil.Process(state.identity.pid).status()==psutil.STATUS_ZOMBIE
    assert signals==[(first,signal.SIGTERM)]
    if entry=='starter':assert following==[('PBCluster',a) for a in ('stop','start','restart')]


@pytest.mark.parametrize('action',['stop','restart'])
def test_retired_zombie_api_preserves_unknown_fences(retained_zombie,monkeypatch,action):
    """The actual API cannot turn a retired registration into journal/launch authorization."""
    state=retained_zombie
    coindata_process._write_launch(state.root,{'version':1,'token':'b'*32,'pid':None,'create_time':None})
    before=coindata_process._launch_path(state.root).read_bytes()
    monkeypatch.setattr(services,'PBGDIR',state.root)
    monkeypatch.setattr(services,'_optional_service_blocker',lambda *a:'')
    monkeypatch.setattr(coindata_process,'signal_exact_process',lambda *a:pytest.fail('No signals'))
    with pytest.raises(services.HTTPException) as error:getattr(services,action+'_service')('pbcoindata',session=None)
    assert error.value.status_code==409
    assert coindata_process._launch_path(state.root).read_bytes()==before


@pytest.fixture
def manual_exit_transition(tmp_path, monkeypatch):
    """Exercise real Stop/wait/authorization with only a synthetic kernel and clock."""
    import signal
    from types import SimpleNamespace
    import psutil
    state = SimpleNamespace(root=tmp_path, phase='running', finish='gone', created=123.0,
                            clock=0.0, assignment='original', signals=[], starts=[], cleaned=[], following=[])
    for name in ('PBCoinData.py', 'pbcoindata_guard.py', 'secure_files.py'):
        (tmp_path / name).write_text('# isolated start prerequisite\n')
    class Process:
        """Supply native-shaped fields without reading any real PID."""
        def __init__(self, pid):
            if state.phase in {'gone', 'new'}:
                raise psutil.NoSuchProcess(pid)
        def create_time(self):
            """Expose an explicit reused lifetime when requested."""
            return state.created
        def status(self):
            """The observed empty-argv window still reports running."""
            return psutil.STATUS_RUNNING
        def uids(self):
            """Keep real/effective UID verification in the original inspector."""
            return SimpleNamespace(real=coindata_process.os.getuid(), effective=coindata_process.os.getuid())
        def cmdline(self):
            """Drop argv only after the authorized TERM."""
            return [] if state.phase == 'empty' else ['python', '-u', str(tmp_path / 'PBCoinData.py')]
        def cwd(self):
            """Return only this temporary checkout."""
            return str(tmp_path)
    def observe(root):
        """Use the original inspector for the old lifetime and a confirmed replacement start."""
        if state.phase == 'new':
            return {'status':'running', 'pid':24, 'identity':ProcessIdentity(24,456)}
        return coindata_process.inspect_pid(root, 23, ProcessIdentity(23,123))
    def tick(delay):
        """Advance the bounded exit window without wall-clock sleeps."""
        state.clock += delay
        if state.finish == 'gone': state.phase = 'gone'
        elif state.finish == 'reuse': state.created = 124.0
        elif state.finish == 'assignment':
            state.phase, state.assignment = 'running', 'changed'
    def send(identity, signum):
        """Record signals; no actual target exists."""
        state.signals.append((identity, signum))
        state.phase = 'empty'
    def start(root, *, authorization):
        """Only a confirmed exit and fresh start proof can continue Restart."""
        assert state.phase == 'gone'
        coindata_process._consume_authorization(root, 'start', authorization)
        state.starts.append('start')
        state.phase = 'new'
    monkeypatch.setattr(coindata_process.psutil, 'Process', Process)
    monkeypatch.setattr(coindata_process.time, 'monotonic', lambda:state.clock)
    monkeypatch.setattr(coindata_process.time, 'sleep', tick)
    monkeypatch.setattr(coindata_process, 'manager_state', lambda *a:{'manager':'legacy','reason':'isolated confirmed manager'})
    monkeypatch.setattr(coindata_process, 'snapshot', observe)
    monkeypatch.setattr(coindata_process, 'signal_exact_process', send)
    monkeypatch.setattr(coindata_process, 'reap_children', lambda:None)
    monkeypatch.setattr(coindata_process, 'clear_owned_files', lambda root,identity:state.cleaned.append(identity))
    monkeypatch.setattr(coindata_process, 'start', start)
    monkeypatch.setattr('pbcoindata_manager.manual_process_evidence', lambda *a:state.assignment)
    monkeypatch.setattr(services, 'PBGDIR', tmp_path)
    monkeypatch.setattr(services, '_optional_service_blocker', lambda *a:'')
    monkeypatch.setattr(services, '_service_status', lambda *a:{'running':state.phase in {'running','empty','new'},'manager':'legacy'})
    lease = coindata_process.FileLease(tmp_path, 'controller', 'pbrun')
    try:
        yield state
    finally:
        lease.release()


@pytest.mark.parametrize('entry', ['api', 'starter'])
@pytest.mark.parametrize('action', ['stop', 'restart'])
@pytest.mark.parametrize('finish', ['gone', 'empty', 'reuse', 'assignment'])
def test_manual_exit_transition_results(manual_exit_transition, monkeypatch, capsys, entry, action, finish):
    """API/Starter wait past empty argv, or refuse unsafe escalation and replacement starts."""
    import signal
    import starter
    state = manual_exit_transition
    state.finish = finish
    if entry == 'api':
        handler = getattr(services, action + '_service')
        if finish == 'gone':
            result = handler('pbcoindata', session=None)
            assert result['running'] == (action == 'restart')
        else:
            with pytest.raises(services.HTTPException) as error:
                handler('pbcoindata', session=None)
            assert error.value.status_code == 409
    else:
        monkeypatch.chdir(state.root)
        monkeypatch.setattr(sys, 'argv', ['starter.py', '--'+action, 'PBCoinData', 'PBCluster'])
        monkeypatch.setattr(starter, '_systemd_action', lambda service,requested:(state.following.append(service) or True,True))
        if finish == 'gone': starter.main()
        else:
            with pytest.raises(SystemExit) as error: starter.main()
            assert error.value.code == 1
            assert 'PBCoinData '+action+' failed' in capsys.readouterr().err
        assert state.following == ['PBCluster']
    assert state.signals == [(ProcessIdentity(23,123), signal.SIGTERM)]
    assert state.starts == (['start'] if finish == 'gone' and action == 'restart' else [])
    assert bool(state.cleaned) == (finish == 'gone')
    assert 0 < state.clock <= 10.000001


def test_service_lifecycle_lock_is_reentrant(tmp_path: Path) -> None:
    """One thread may safely nest the same per-service lifecycle lease."""
    outer = acquire_service_lifecycle_lock(tmp_path, "pbdata", "restart", timeout=0.2)
    inner = acquire_service_lifecycle_lock(tmp_path, "pbdata", "stop", timeout=0.2)

    inner.release()
    outer.release()

    replacement = acquire_service_lifecycle_lock(tmp_path, "pbdata", "start", timeout=0.2)
    replacement.release()


@pytest.mark.parametrize("action", ["start", "stop", "restart"])
def test_manual_coindata_api_action_ignores_held_controller_lease(tmp_path, monkeypatch, action):
    """Election ownership never blocks explicit serialized API service actions."""
    (tmp_path / "PBCoinData.py").write_text("# synthetic guarded start path\n")
    (tmp_path / "pbcoindata_guard.py").write_text("# synthetic guard\n")
    (tmp_path / "secure_files.py").write_text("# synthetic stdlib helper\n")
    lease = coindata_process.FileLease(tmp_path, "controller", "pbrun")
    monkeypatch.setattr(coindata_process, "manager_state", lambda root: {"manager": "legacy", "reason": "synthetic legacy"})
    state = {"running": action != "start", "generation": 1}
    calls = []

    def status(name):
        """Return only a synthetic local service state."""
        return {"running": state["running"], "manager": "legacy"}

    def stop(root, *, authorization):
        """Nested action locks must be reentrant rather than request election."""
        with coindata_process.actions(tmp_path, "stop"):
            calls.append("stop")
            state["running"] = False

    def start(root, *, authorization):
        """Complete the synthetic start without opening subprocesses."""
        with coindata_process.actions(tmp_path, "start"):
            calls.append("start")
            state.update(running=True, generation=2)

    monkeypatch.setattr(services, "PBGDIR", tmp_path)
    monkeypatch.setattr(services, "_optional_service_blocker", lambda name: "")
    monkeypatch.setattr(services, "_service_status", status)
    monkeypatch.setattr(services, "_service_identities", lambda *a: ((23, state["generation"]),) if state["running"] else ())
    monkeypatch.setattr(coindata_process, "stop", stop)
    monkeypatch.setattr(coindata_process, "start", start)
    monkeypatch.setattr(coindata_process, "snapshot", lambda root: {"status": "running" if state["running"] else "stopped", "pid": 23 if state["running"] else None, "identity": ProcessIdentity(23, state["generation"]) if state["running"] else None})
    monkeypatch.setattr("pbcoindata_manager.manual_process_evidence", lambda *a: None)
    try:
        services._service_action("pbcoindata", action)
        assert calls == ({"start": ["start"], "stop": ["stop"], "restart": ["stop", "start"]}[action])
    finally:
        lease.release()


def test_starter_serializes_entire_manual_coindata_restart(tmp_path, monkeypatch):
    """Starter uses action locks and the atomic wrapper, never election or stop/start gaps."""
    import starter
    calls = []
    (tmp_path / "PBCoinData.py").write_text("# synthetic guarded start path\n")
    (tmp_path / "pbcoindata_guard.py").write_text("# synthetic guard\n")
    (tmp_path / "secure_files.py").write_text("# synthetic stdlib helper\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["starter.py", "--restart", "PBCoinData"])
    monkeypatch.setattr(coindata_process, "manager_state", lambda root: {"manager": "legacy", "reason": "synthetic legacy"})

    def transition(root, *, authorization):
        """A competing thread must remain excluded for the whole wrapper action."""
        errors = []
        def compete():
            """Try a bounded action lock from another thread."""
            try:
                acquire_service_lifecycle_lock(tmp_path, "all-services", "compete", timeout=0.01).release()
            except ServiceLifecycleBusyError:
                errors.append("busy")
        thread = threading.Thread(target=compete)
        thread.start()
        thread.join(timeout=1)
        assert errors == ["busy"]
        calls.append(authorization.action)

    monkeypatch.setattr(coindata_process, "snapshot", lambda root: {"status": "stopped"})
    monkeypatch.setattr("pbcoindata_manager.manual_process_evidence", lambda *a: None)
    monkeypatch.setattr(coindata_process, "start", transition)
    monkeypatch.setattr(coindata_process, "stop", transition)
    election = coindata_process.FileLease(tmp_path, "controller", "pbrun")
    try:
        starter.main()
        assert calls == ["stop", "start"]
    finally:
        election.release()


@pytest.mark.parametrize("manager", ["unknown", "systemd"])
@pytest.mark.parametrize("action", ["start", "stop", "restart"])
def test_coindata_api_manager_guard_prevents_legacy_fallback(tmp_path, monkeypatch, manager, action):
    """API actions freshly enforce manager findings instead of trusting old PID status."""
    monkeypatch.setattr(services, "PBGDIR", tmp_path)
    monkeypatch.setattr(services, "_optional_service_blocker", lambda name: "")
    monkeypatch.setattr(coindata_process, "manager_state", lambda root: {"manager": manager, "reason": "synthetic unknown manager"})
    monkeypatch.setattr(services, "_service_status", lambda *a: {"manager": "legacy", "running": action != "start"})
    monkeypatch.setattr(services, "_service_identities", lambda *a: ())
    monkeypatch.setattr(services, "_systemd_service_action", lambda *a: None)
    monkeypatch.setattr(services, "_get_service", lambda *a: pytest.fail("No legacy fallback allowed"))
    monkeypatch.setattr(coindata_process, "snapshot", lambda root: {"status": "unknown", "error": "synthetic inaccessible identity"})
    with pytest.raises(services.HTTPException) as error:
        services._service_action("pbcoindata", action)
    assert error.value.status_code == 409


@pytest.mark.parametrize("missing", [False, True])
def test_starter_uses_positive_offline_proof_without_user_bus(tmp_path, monkeypatch, missing):
    """An unavailable bus is usable only after the separate complete evidence check."""
    import starter
    import pbcoindata_manager as manager
    calls = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["starter.py", "--start", "PBCoinData", "PBCluster"])
    monkeypatch.setattr(manager, "_unit_paths", lambda *a: ([tmp_path], set()))
    monkeypatch.setattr(manager, "_offline_manager_absent", lambda *a: None)
    def query(*a, **kw):
        """Never call a real service manager."""
        if missing:
            raise FileNotFoundError("synthetic systemctl absence")
        return subprocess.CompletedProcess(a, 1, stdout="", stderr="synthetic bus absence")
    monkeypatch.setattr(manager.subprocess, "run", query)
    monkeypatch.setattr(starter, "_systemd_action", lambda *a: (False, False))
    monkeypatch.setattr(starter, "_legacy_action", lambda *a: calls.append(a))
    monkeypatch.setattr(coindata_process, "snapshot", lambda root: {"status": "stopped"})
    monkeypatch.setattr(manager, "manual_process_evidence", lambda *a: None)
    monkeypatch.setattr(coindata_process, "start", lambda root, **kw: calls.append(("PBCoinData", "start")))
    starter.main()
    assert calls == [("PBCoinData", "start"), ("PBCluster", "start")]


@pytest.mark.parametrize("flag", ["--start", "--stop", "--restart"])
@pytest.mark.parametrize("failure", ["no-bus", "missing-command", "action-error"])
def test_starter_coindata_failure_preserves_following_service(tmp_path, monkeypatch, capsys, flag, failure):
    """One failed CoinData action cannot suppress another explicitly requested service."""
    import starter
    for name in ('PBCoinData.py', 'pbcoindata_guard.py', 'secure_files.py'):
        (tmp_path / name).write_text('# isolated entry dependency\n')
    calls = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["starter.py", flag, "PBCoinData", "PBCluster"])
    if failure == "action-error":
        monkeypatch.setattr(coindata_process, "manager_state", lambda root: {"manager": "legacy", "reason": "synthetic legacy"})
    else:
        import pbcoindata_manager as manager
        monkeypatch.setattr(manager, "_unit_paths", lambda *a: ([tmp_path], set()))
        monkeypatch.setattr(manager, "_offline_manager_absent", lambda *a: (_ for _ in ()).throw(ValueError("synthetic unknown")))

        def query(*args, **kwargs):
            """Represent bus absence or a missing executable without contacting systemd."""
            if failure == "missing-command":
                raise FileNotFoundError("synthetic missing systemctl")
            return subprocess.CompletedProcess(args, 1, stdout="", stderr="synthetic unavailable bus")

        monkeypatch.setattr(coindata_process.subprocess, "run", query)
    monkeypatch.setattr(starter, "_systemd_action", lambda *args: (False, False))

    def action(service, requested):
        """Record only synthetic service actions, failing CoinData before any process work."""
        calls.append((service, requested))
        if service == "PBCoinData":
            raise coindata_process.ProcessStateError("synthetic unconfirmed transition")

    monkeypatch.setattr(starter, "_legacy_action", action)
    monkeypatch.setattr(coindata_process, "snapshot", lambda root: {"status": "stopped"} if failure == "action-error" else {"status": "unknown", "error": "synthetic unavailable process evidence"})
    monkeypatch.setattr("pbcoindata_manager.manual_process_evidence", lambda *a: None)
    monkeypatch.setattr(coindata_process, "start", lambda root, **kw: action("PBCoinData", "start"))
    monkeypatch.setattr(coindata_process, "stop", lambda root, **kw: action("PBCoinData", "stop"))
    with pytest.raises(SystemExit) as error:
        starter.main()
    assert error.value.code == 1
    expected_action = {"--start": "start", "--stop": "stop", "--restart": "restart"}[flag]
    assert calls == ([('PBCoinData', 'stop' if expected_action == 'restart' else expected_action)] if failure == 'action-error' else []) + [('PBCluster', expected_action)]
    assert "PBCoinData" in capsys.readouterr().err


def test_migration_aborts_unconfirmed_legacy_coindata_stop(monkeypatch):
    """An explicitly requested migration cannot replace an unconfirmed own daemon."""
    monkeypatch.setattr(services, "_MIGRATION_LEGACY_STOP_SERVICES", ["pbcoindata"])
    monkeypatch.setattr(services, "_systemd_service_status", lambda name: None)
    def denied():
        """Simulate ambiguous exact process ownership."""
        raise coindata_process.ProcessStateError("denied")
    monkeypatch.setattr(services, "_get_service", lambda name: type("Service", (), {"is_running": staticmethod(denied)})())
    with pytest.raises(RuntimeError, match="unconfirmed"):
        services._stop_legacy_services([])


def test_service_lifecycle_lease_can_release_from_handoff_thread(tmp_path: Path) -> None:
    """A restart watchdog may release a lease acquired by the request thread."""
    lease = acquire_service_lifecycle_lock(tmp_path, "api-server", "restart", timeout=0.2)
    releaser = threading.Thread(target=lease.release)
    releaser.start()
    releaser.join(timeout=2)

    assert not releaser.is_alive()
    replacement = acquire_service_lifecycle_lock(tmp_path, "api-server", "restart", timeout=0.2)
    replacement.release()


def test_detached_lifecycle_lease_disables_request_thread_reentry(tmp_path: Path) -> None:
    """A reused request thread cannot reenter a lease handed to async restart work."""
    lease = acquire_service_lifecycle_lock(tmp_path, "api-server", "restart", timeout=0.2)
    lease.detach()
    try:
        with pytest.raises(ServiceLifecycleBusyError):
            acquire_service_lifecycle_lock(tmp_path, "api-server", "restart", timeout=0.05)
    finally:
        lease.release()


def test_service_lifecycle_lock_reports_cross_process_contention(tmp_path: Path) -> None:
    """A competing process receives a bounded error with owner context."""
    script = """
import sys
from pathlib import Path
from service_lifecycle_lock import acquire_service_lifecycle_lock
lease = acquire_service_lifecycle_lock(Path(sys.argv[1]), 'pbdata', 'restart', timeout=1)
print('locked', flush=True)
sys.stdin.readline()
lease.release()
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "locked"
        with pytest.raises(ServiceLifecycleBusyError) as error:
            acquire_service_lifecycle_lock(tmp_path, "pbdata", "start", timeout=0.05)
        assert "pbdata" in str(error.value)
        assert "pid=" in str(error.value)
        assert "0.1s" in str(error.value)
    finally:
        if child.stdin is not None:
            child.stdin.write("release\n")
            child.stdin.flush()
        child.wait(timeout=5)


def test_legacy_start_is_serialized_and_revalidated(monkeypatch, tmp_path: Path) -> None:
    """Two starts serialize and the second observes the first daemon instead of spawning."""
    state = {"running": False, "pid": 0, "runs": 0}
    run_entered = threading.Event()
    allow_run = threading.Event()

    class Service:
        """Controllable legacy service double."""

        def run(self) -> None:
            state["runs"] += 1
            run_entered.set()
            assert allow_run.wait(timeout=2)
            state.update(running=True, pid=101)

        def stop(self) -> None:
            state.update(running=False, pid=0)

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_optional_service_blocker", lambda _name: "")
    monkeypatch.setattr(services, "_systemd_service_action", lambda _name, _action: None)
    monkeypatch.setattr(services, "_service_status", lambda _name: {"running": state["running"], "manager": "legacy"})
    monkeypatch.setattr(services, "_service_identities", lambda _name, status=None: ((state["pid"], float(state["pid"])),) if state["pid"] else ())
    monkeypatch.setattr(services, "_get_service", lambda _name: Service())

    results: list[dict[str, object]] = []
    first = threading.Thread(target=lambda: results.append(services._service_action("pbdata", "start")))
    second = threading.Thread(target=lambda: results.append(services._service_action("pbdata", "start")))
    first.start()
    assert run_entered.wait(timeout=2)
    second.start()
    time.sleep(0.05)
    assert state["runs"] == 1
    allow_run.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert state["runs"] == 1
    assert len(results) == 2
    assert all(result["running"] is True for result in results)


def test_restart_waits_for_old_identity_before_start(monkeypatch, tmp_path: Path) -> None:
    """Legacy restart verifies shutdown and a distinct replacement without a fixed delay."""
    state = {"running": True, "pid": 10}
    calls: list[str] = []

    class Service:
        """Legacy service double with explicit process identity changes."""

        def stop(self) -> None:
            calls.append("stop")
            state.update(running=False, pid=0)

        def run(self) -> None:
            calls.append("run")
            state.update(running=True, pid=20)

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_optional_service_blocker", lambda _name: "")
    monkeypatch.setattr(services, "_systemd_service_action", lambda _name, _action: None)
    monkeypatch.setattr(services, "_service_status", lambda _name: {"running": state["running"], "manager": "legacy"})
    monkeypatch.setattr(services, "_service_identities", lambda _name, status=None: ((state["pid"], float(state["pid"])),) if state["pid"] else ())
    monkeypatch.setattr(services, "_get_service", lambda _name: Service())

    result = services._service_action("pbdata", "restart")

    assert calls == ["stop", "run"]
    assert result["running"] is True
    assert state["pid"] == 20


def test_systemd_action_remains_inside_lifecycle_lock(monkeypatch, tmp_path: Path) -> None:
    """The per-service lease covers the complete systemd command and verification."""
    state = {"running": False}
    contention: list[bool] = []

    def systemd_action(_name: str, _action: str) -> dict[str, object]:
        def contend() -> None:
            try:
                acquire_service_lifecycle_lock(tmp_path, "pbdata", "stop", timeout=0.05)
            except ServiceLifecycleBusyError:
                contention.append(True)

        competitor = threading.Thread(target=contend)
        competitor.start()
        competitor.join(timeout=2)
        state["running"] = True
        return {"running": True, "manager": "systemd", "unit": "pbgui-pbdata.service"}

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_optional_service_blocker", lambda _name: "")
    monkeypatch.setattr(
        services,
        "_service_status",
        lambda _name: {
            "running": state["running"],
            "manager": "systemd",
            "unit": "pbgui-pbdata.service",
        },
    )
    monkeypatch.setattr(
        services,
        "_service_identities",
        lambda _name, status=None: ((200, 2.0),) if state["running"] else (),
    )
    monkeypatch.setattr(services, "_systemd_service_action", systemd_action)

    result = services._service_action("pbdata", "start")

    assert result["running"] is True
    assert contention == [True]


def test_lifecycle_contention_remains_http_409(monkeypatch, tmp_path: Path) -> None:
    """Route error handling preserves lifecycle lock conflicts as HTTP 409."""
    locked = threading.Event()
    release = threading.Event()

    def hold_lock() -> None:
        with acquire_service_lifecycle_lock(tmp_path, "pbdata", "restart", timeout=1):
            locked.set()
            assert release.wait(timeout=2)

    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert locked.wait(timeout=2)
    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_SERVICE_LIFECYCLE_LOCK_TIMEOUT_S", 0.05)
    try:
        with pytest.raises(services.HTTPException) as error:
            services.start_service("pbdata", session=None)
        assert error.value.status_code == 409
        assert "Another lifecycle action" in str(error.value.detail)
    finally:
        release.set()
        holder.join(timeout=2)


def test_api_restart_lifecycle_contention_remains_http_409(monkeypatch) -> None:
    """The dedicated API restart route preserves lifecycle contention as HTTP 409."""
    monkeypatch.setattr(
        PBApiServer,
        "_acquire_api_restart_leases",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ServiceLifecycleBusyError("restart busy")),
    )

    with pytest.raises(services.HTTPException) as error:
        services.restart_api_server(session=None)

    assert error.value.status_code == 409
    assert "restart busy" in str(error.value.detail)


def test_pbcluster_stop_rejects_unrelated_pid(monkeypatch, tmp_path: Path) -> None:
    """PBCluster never signals a PID that fails exact script identity validation."""
    service = PBCluster(tmp_path)
    service.pidfile.parent.mkdir(parents=True)
    service.pidfile.write_text("424242\n", encoding="utf-8")
    monkeypatch.setattr(service, "_process_identity", lambda _pid: None)
    monkeypatch.setattr("PBCluster.signal_exact_process", lambda *_args: (_ for _ in ()).throw(AssertionError("must not signal")))

    service.stop()

    assert service.pidfile.read_text(encoding="utf-8") == "424242\n"


def test_pbcluster_stop_rejects_same_pid_with_reused_creation_time(monkeypatch, tmp_path: Path) -> None:
    """PBCluster sidecar identity prevents signalling a same-script recycled PID."""
    service = PBCluster(tmp_path)
    service.pidfile.parent.mkdir(parents=True)
    service.pidfile.write_text("4242\n", encoding="utf-8")
    write_process_identity(service.pidfile, ProcessIdentity(4242, 10.0))
    monkeypatch.setattr(service, "_process_identity", lambda _pid: ProcessIdentity(4242, 11.0))
    monkeypatch.setattr("PBCluster.signal_exact_process", lambda *_args: (_ for _ in ()).throw(AssertionError("must not signal")))

    service.stop()

    assert service.pidfile.read_text(encoding="utf-8") == "4242\n"


def test_restart_accepts_same_pid_with_new_creation_time(monkeypatch) -> None:
    """A recycled numeric PID with a new create time is a valid replacement."""
    identities = iter([((42, 10.0),), ((42, 11.0),)])
    monkeypatch.setattr(services, "_service_status", lambda _name: {"running": True, "manager": "systemd"})
    monkeypatch.setattr(services, "_service_identities", lambda _name, status=None: next(identities))

    result = services._wait_for_service_transition(
        "pbdata",
        running=True,
        previous_identities=((42, 10.0),),
    )

    assert result["running"] is True


def test_global_transition_lock_blocks_per_service_action(monkeypatch, tmp_path: Path) -> None:
    """A root restart reservation excludes a concurrent individual transition."""
    lease = acquire_service_lifecycle_lock(tmp_path, "all-services", "root-restart", timeout=0.2)
    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_SERVICE_LIFECYCLE_LOCK_TIMEOUT_S", 0.05)
    outcome: list[int] = []

    def contend() -> None:
        try:
            services._service_action("pbdata", "start")
        except services.HTTPException as exc:
            outcome.append(exc.status_code)

    competitor = threading.Thread(target=contend)
    competitor.start()
    competitor.join(timeout=2)
    lease.release()

    assert outcome == [409]


def test_exact_owner_signal_failure_maps_to_http_409(monkeypatch, tmp_path: Path) -> None:
    """A live exact daemon after signal failure is a conflict, never stop success."""
    class Service:
        """Legacy service double that cannot deliver its exact stop signal."""

        def stop(self) -> None:
            raise ExactProcessSignalError("exact owner remains alive")

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(services, "_service_status", lambda _name: {"running": True, "manager": "legacy"})
    monkeypatch.setattr(services, "_service_identities", lambda _name, status=None: ((42, 10.0),))
    monkeypatch.setattr(services, "_get_service", lambda _name: Service())

    with pytest.raises(services.HTTPException) as error:
        services._service_action("pbcluster", "stop")

    assert error.value.status_code == 409
    assert "remains alive" in str(error.value.detail)


def test_pbcluster_foreground_suppresses_duplicate(monkeypatch, tmp_path: Path) -> None:
    """A second legacy PBCluster process leaves the authoritative PID untouched."""
    service = PBCluster(tmp_path)
    service.pidfile.parent.mkdir(parents=True)
    service.pidfile.write_text("12345\n", encoding="utf-8")
    monkeypatch.setattr("PBCluster._log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_pid_matches", lambda pid: pid == 12345)
    monkeypatch.setattr(
        service.worker,
        "run_forever",
        lambda: (_ for _ in ()).throw(AssertionError("duplicate must not run")),
    )

    service.run_foreground()

    assert service.pidfile.read_text(encoding="utf-8") == "12345\n"


def test_systemd_migration_holds_global_then_update_lease_for_full_transaction(
    monkeypatch, tmp_path: Path,
) -> None:
    """Migration reserves all services before master update and releases in reverse."""
    events: list[str] = []

    class Lease:
        """Record acquisition-specific release ordering."""

        def __init__(self, name: str) -> None:
            self.name = name

        def release(self) -> None:
            events.append(f"release:{self.name}")

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(
        services,
        "acquire_service_lifecycle_lock",
        lambda _root, service, action, timeout: (
            events.append(f"acquire:{service}:{action}") or Lease("all-services")
        ),
    )
    monkeypatch.setattr(
        services,
        "acquire_master_update_lock",
        lambda _root: events.append("acquire:master-update") or Lease("master-update"),
    )

    def migrate() -> dict[str, bool]:
        events.append("migration")
        assert events == ["acquire:all-services:migration", "acquire:master-update", "migration"]
        return {"ok": True}

    monkeypatch.setattr(services, "_run_systemd_migration", migrate)

    assert services.run_migration(session=None) == {"ok": True}
    assert events == [
        "acquire:all-services:migration",
        "acquire:master-update",
        "migration",
        "release:master-update",
        "release:all-services",
    ]


@pytest.mark.parametrize("busy_kind", ["lifecycle", "master-update"])
def test_systemd_migration_lock_contention_is_http_409(
    monkeypatch, tmp_path: Path, busy_kind: str,
) -> None:
    """Migration lock contention remains a retryable conflict and releases partial leases."""
    released: list[str] = []

    class Lease:
        """Track release of an acquired global lease."""

        def release(self) -> None:
            released.append("all-services")

    monkeypatch.setattr(services, "PBGDIR", str(tmp_path))
    if busy_kind == "lifecycle":
        monkeypatch.setattr(
            services,
            "acquire_service_lifecycle_lock",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(ServiceLifecycleBusyError("busy")),
        )
    else:
        monkeypatch.setattr(services, "acquire_service_lifecycle_lock", lambda *_args, **_kwargs: Lease())
        monkeypatch.setattr(
            services,
            "acquire_master_update_lock",
            lambda _root: (_ for _ in ()).throw(services.MasterUpdateBusyError("busy")),
        )
    monkeypatch.setattr(
        services,
        "_run_systemd_migration",
        lambda: (_ for _ in ()).throw(AssertionError("migration must not run")),
    )

    with pytest.raises(services.HTTPException) as error:
        services.run_migration(session=None)

    assert error.value.status_code == 409
    assert released == ([] if busy_kind == "lifecycle" else ["all-services"])


@pytest.mark.parametrize('entry', ['api', 'starter'])
def test_manual_restart_requires_new_start_authorization(tmp_path, monkeypatch, entry):
    """A permitted stop cannot authorize an unsafe following start, even with a held lease."""
    import starter
    import pbcoindata_manager as manager
    calls, state = [], {'running': True}
    monkeypatch.setattr(coindata_process, 'manager_state', lambda root: {'manager': 'unknown', 'reason': 'synthetic missing tools'})
    monkeypatch.setattr(coindata_process, 'snapshot', lambda root: {'status': 'running' if state['running'] else 'stopped', 'pid': 23 if state['running'] else None, 'identity': ProcessIdentity(23, 123) if state['running'] else None})
    def evidence(root, action, actual, finding):
        """Permit only the exact stop; the new start has incomplete process visibility."""
        calls.append('authorize-' + action)
        if action == 'start':
            raise ValueError('synthetic hidden process namespace')
    def stop(root, *, authorization):
        """Consume the explicit stop proof without signalling a real process."""
        coindata_process._consume_authorization(root, 'stop', authorization)
        calls.append('stop')
        state['running'] = False
    monkeypatch.setattr(manager, 'manual_process_evidence', evidence)
    monkeypatch.setattr(coindata_process, 'stop', stop)
    monkeypatch.setattr(coindata_process, 'start', lambda *a, **k: pytest.fail('Start must not run'))
    lease = coindata_process.FileLease(tmp_path, 'controller', 'pbrun')
    try:
        if entry == 'api':
            monkeypatch.setattr(services, 'PBGDIR', tmp_path)
            monkeypatch.setattr(services, '_optional_service_blocker', lambda name: '')
            monkeypatch.setattr(services, '_systemd_service_action', lambda *a: pytest.fail('UNKNOWN is not systemd'))
            with pytest.raises(services.HTTPException) as error:
                services._service_action('pbcoindata', 'restart')
            assert error.value.status_code == 409
        else:
            monkeypatch.chdir(tmp_path)
            monkeypatch.setattr(sys, 'argv', ['starter.py', '--restart', 'PBCoinData'])
            monkeypatch.setattr(starter, '_systemd_action', lambda *a: pytest.fail('UNKNOWN is not systemd'))
            with pytest.raises(SystemExit) as error:
                starter.main()
            assert error.value.code == 1
        assert calls == ['authorize-start']
        assert state['running'] is True
    finally:
        lease.release()


def test_coindata_status_keeps_unknown_after_manual_success(tmp_path, monkeypatch):
    """Exact running state and identity must not relabel an UNKNOWN manager as legacy."""
    monkeypatch.setattr(services, 'PBGDIR', tmp_path)
    monkeypatch.setattr(coindata_process, 'manager_state', lambda root: {'manager': 'unknown', 'reason': 'synthetic unavailable automatic proof'})
    monkeypatch.setattr(coindata_process, 'snapshot', lambda root: {'status': 'running', 'pid': 23, 'identity': ProcessIdentity(23, 123)})
    monkeypatch.setattr(services, '_systemd_enable_status', lambda *a: {})
    status = services._service_status('pbcoindata')
    assert status['running'] is True and status['manager'] == 'unknown'
    assert services._service_identities('pbcoindata', status) == ((23, 123),)


@pytest.mark.parametrize('entry', ['api', 'starter'])
def test_restart_later_start_refusal_reports_confirmed_stopped(tmp_path, monkeypatch, capsys, entry):
    """A new obstacle after a successful preflight is reported as a failed stopped restart."""
    import starter
    state, calls = {'running': True}, []
    for name in ('PBCoinData.py', 'pbcoindata_guard.py', 'secure_files.py'):
        (tmp_path / name).write_text('# isolated entry dependency\n')
    monkeypatch.setattr(coindata_process, 'manager_state', lambda root: {'manager': 'unknown'})
    monkeypatch.setattr(coindata_process, 'snapshot', lambda root, **kwargs: {'status': 'running' if state['running'] else 'stopped', 'pid': 23 if state['running'] else None, 'identity': ProcessIdentity(23,123) if state['running'] else None})
    monkeypatch.setattr(coindata_process,'read_systemd_guard',lambda root,deadline:coindata_process.GuardObservation(root,None,b'',None))
    def evidence(root, action, actual, finding):
        """The start was viable before stop, but becomes unsafe afterward."""
        calls.append(action)
        if action == 'start' and not state['running']:
            raise ValueError('isolated later obstacle')
    monkeypatch.setattr('pbcoindata_manager.manual_process_evidence', evidence)
    monkeypatch.setattr(coindata_process, 'stop', lambda *a, **k: state.update(running=False))
    monkeypatch.setattr(coindata_process, 'start', lambda *a, **k: pytest.fail('No unsafe spawn'))
    if entry == 'api':
        monkeypatch.setattr(services, 'PBGDIR', tmp_path)
        monkeypatch.setattr(services, '_optional_service_blocker', lambda *a: '')
        with pytest.raises(services.HTTPException) as error:
            services._service_action('pbcoindata', 'restart')
        assert error.value.status_code == 409 and 'is stopped' in error.value.detail
    else:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, 'argv', ['starter.py', '--restart', 'PBCoinData'])
        with pytest.raises(SystemExit): starter.main()
        assert 'is stopped' in capsys.readouterr().err
    assert not state['running'] and calls == ['start', 'stop', 'start']


@pytest.mark.parametrize('entry', ['api', 'starter'])
@pytest.mark.parametrize('fault', ['missing-helper', 'unreadable-script'])
def test_systemd_restart_preflight_preserves_running_on_entry_failure(tmp_path, monkeypatch, entry, fault):
    """Confirmed systemd routing still checks known entry blockers before any stop."""
    import starter
    for name in ('PBCoinData.py', 'pbcoindata_guard.py', 'secure_files.py'):
        (tmp_path / name).write_text('# isolated dependency\n')
    monkeypatch.setattr(coindata_process, 'manager_state', lambda root: {'manager': 'systemd'})
    if fault == 'missing-helper': (tmp_path / 'pbcoindata_guard.py').unlink()
    original = coindata_process.os.access
    if fault == 'unreadable-script':
        monkeypatch.setattr(coindata_process.os, 'access', lambda path, mode: False if Path(path) == tmp_path / 'PBCoinData.py' else original(path, mode))
    if entry == 'api':
        monkeypatch.setattr(services, 'PBGDIR', tmp_path)
        monkeypatch.setattr(services, '_optional_service_blocker', lambda *a: '')
        monkeypatch.setattr(services, '_systemd_service_action', lambda *a: pytest.fail('No avoidable systemd stop'))
        with pytest.raises(services.HTTPException) as error: services._service_action('pbcoindata', 'restart')
        assert error.value.status_code == 409
    else:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(sys, 'argv', ['starter.py', '--restart', 'PBCoinData'])
        monkeypatch.setattr(starter, '_systemd_action', lambda *a: pytest.fail('No avoidable systemd stop'))
        with pytest.raises(SystemExit) as error: starter.main()
        assert error.value.code == 1
