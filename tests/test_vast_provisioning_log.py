"""Offline provisioning log download boundaries."""
from types import SimpleNamespace
import io
import pytest
import vast_provisioning_log as logs


def test_manifest_weighted_progress_preserves_rolling_tail():
    """Large completed layers outweigh tiny ones; rolling logs preserve completion."""
    from vast_image_layers import IMAGE_LAYERS
    image, sizes = next(iter(IMAGE_LAYERS.items()))
    progress = logs.image_layer_progress('ea381c80ad7f: Download complete\n02a27392fe49: Already exists', image=image)
    assert progress['total'] == 27
    assert progress['completed_bytes'] == 820822233 + 250
    assert progress['total_bytes'] == sum(sizes.values())
    assert progress['download_percent'] == pytest.approx(100 * (820822233 + 250) / sum(sizes.values()))
    assert progress['ready'] == 1
    later = logs.image_layer_progress('ea381c80ad7f: Pull complete\nffffffffffff: Pull complete', progress, image=image)
    assert later['completed_bytes'] == progress['completed_bytes']
    assert later['ready'] == 2
    assert 'completed_bytes' not in logs.image_layer_progress('', later, image='unknown')


def test_manifest_complete_download_is_not_ready():
    """All compressed bytes can be available while layers still need extraction."""
    from vast_image_layers import IMAGE_LAYERS
    image, sizes = next(iter(IMAGE_LAYERS.items()))
    text = '\n'.join(f'{key}: Download complete' for key in sizes)
    progress = logs.image_layer_progress(text, image=image)
    assert progress['download_percent'] == 100
    assert progress['ready'] == 0


@pytest.mark.parametrize('url,allowed', [('https://s3.amazonaws.com/vast.ai/instance_logs/abc.log', True), ('https://s3.amazonaws.com/public.vast.ai/instance_logs/abc.log', True), ('http://127.0.0.1/private', False)])
def test_provider_log_download_is_bounded_and_throttled(tmp_path, monkeypatch, url, allowed):
    """Only the provider log bucket may be fetched; repeated polls use a cooldown."""
    state = {}
    calls = []
    store = SimpleNamespace(read=lambda _: state, update=lambda _, **kw: state.update(kw))
    client = SimpleNamespace(request=lambda *args: calls.append(args) or {'result_url':url})
    downloads = []
    def download(url, timeout):
        """Serve a local fixture without HTTP."""
        downloads.append(url)
        return io.BytesIO(b'Pulling layer\n')
    monkeypatch.setattr(logs, 'LOG_ROOT', tmp_path/'logs')
    monkeypatch.setattr(logs.urllib.request, 'build_opener', lambda *_: SimpleNamespace(open=download))
    logs.collect_provisioning_log(store, 'a'*32, client, 123)
    logs.collect_provisioning_log(store, 'a'*32, client, 123)
    assert len(calls) == 1
    assert calls[0][1] == '/instances/request_logs/123/'
    assert bool(downloads) is allowed
    if allowed:
        assert (tmp_path/'logs'/('vast_'+'a'*32+'_provider.log')).read_text() == 'Pulling layer\n'


def test_s3_upload_delay_retries_same_url(tmp_path, monkeypatch):
    """An initial missing S3 object must not trigger another Vast API request."""
    import urllib.error
    state = {}
    calls = []
    downloads = []
    url = 'https://s3.amazonaws.com/public.vast.ai/instance_logs/abc.log'
    store = SimpleNamespace(read=lambda _:state, update=lambda _, **kw:state.update(kw))
    client = SimpleNamespace(request=lambda *args:calls.append(args) or {'result_url':url})
    def download(location, timeout):
        """Serve one temporary S3 denial followed by the prepared log."""
        downloads.append(location)
        if len(downloads) == 1:
            raise urllib.error.HTTPError(location,403,'Not ready',{},None)
        return io.BytesIO(b'Pull complete\n')
    monkeypatch.setattr(logs,'LOG_ROOT',tmp_path/'logs')
    monkeypatch.setattr(logs.time,'sleep',lambda _:None)
    monkeypatch.setattr(logs.urllib.request,'build_opener',lambda *_:SimpleNamespace(open=download))
    logs.collect_provisioning_log(store,'a'*32,client,123)
    assert len(calls) == 1
    assert downloads == [url,url]


@pytest.mark.parametrize('existing,fallback', [(None, 'Container starting\n'), (None, ''), ('Pull complete\n', '')])
def test_missing_daemon_log_falls_back_without_losing_snapshot(tmp_path, monkeypatch, existing, fallback):
    """Missing provider files cannot replace useful output with a shell error."""
    identifier = 'a'*32
    path = tmp_path / f'vast_{identifier}_provider.log'
    if existing:
        path.write_text(existing)
    state = {}
    store = SimpleNamespace(read=lambda _:state, update=lambda _, **kw:state.update(kw))
    calls = []
    def download(client, instance_id, *, daemon):
        """Return missing daemon output and an optional container snapshot."""
        calls.append(daemon)
        return 'cat: /var/lib/vastai_kaalia/data/instance_extra_logs/C.123: No such file or directory\n' if daemon else fallback
    monkeypatch.setattr(logs, 'LOG_ROOT', tmp_path)
    monkeypatch.setattr(logs, '_download_log', download)
    logs.collect_provisioning_log(store, identifier, object(), 123)
    logs.collect_provisioning_log(store, identifier, object(), 123)
    assert calls == [True, False]
    result = path.read_text()
    if fallback or existing:
        assert result == (fallback or existing)
    else:
        assert 'waiting for provider logs' in result
        assert 'No such file' not in result


def test_waiting_snapshot_retries_when_worker_becomes_ready(tmp_path, monkeypatch):
    """Worker readiness retries a placeholder without refetching a useful snapshot."""
    identifier = 'a' * 32
    path = tmp_path / f'vast_{identifier}_provider.log'
    path.write_text('2026-09-20T10:45:38 [INFO] Instance 123: waiting for provider logs; automatic retry in 60 seconds.\n')
    state = {'provider_log_checked_at': 1000}
    store = SimpleNamespace(read=lambda _: state, update=lambda _, **kw: state.update(kw))
    calls = []
    monkeypatch.setattr(logs, 'LOG_ROOT', tmp_path)
    monkeypatch.setattr(logs.time, 'time', lambda: 1010)
    monkeypatch.setattr(logs, '_download_log', lambda *_args, **kwargs:
                        calls.append(kwargs['daemon']) or 'Container ready\n')
    logs.collect_provisioning_log(
        store, identifier, object(), 123, retry_waiting=True
    )
    assert calls == [True]
    assert path.read_text() == 'Container ready\n'

    logs.collect_provisioning_log(
        store, identifier, object(), 123, retry_waiting=True
    )
    assert calls == [True]


def test_layer_progress_deduplicates_and_distinguishes_download_from_ready():
    """Mixed provider timestamps and repeated events retain distinct layer states."""
    progress = logs.image_layer_progress('''aaaaaaaaaaaa: Pulling fs layer
bbbbbbbbbbbb: 2026-09-13 19:27:10 UTC: Pulling fs layer
cccccccccccc: Already exists
2026-09-13 19:32:19 UTC: aaaaaaaaaaaa: Download complete
aaaaaaaaaaaa: 2026-09-13 19:32:19 UTC: Download complete
bbbbbbbbbbbb: Verifying Checksum
''')
    assert progress['total'] == 3
    assert progress['downloaded'] == 2
    assert progress['ready'] == 1
    later = logs.image_layer_progress('aaaaaaaaaaaa: Pull complete\nbbbbbbbbbbbb: Download complete\ncccccccccccc: Waiting', progress)
    assert later['total'] == 3
    assert later['ready'] == 2
    assert later['downloaded'] == 3
    assert later['percent'] == pytest.approx(200/3)
    assert logs.image_layer_progress('building container', later) == later
    assert logs.image_layer_progress('No such file or directory') is None


def test_layer_progress_limits_provider_input():
    """An oversized event stream cannot grow persisted layer state without bound."""
    text = '\n'.join(f'{i:012x}: Waiting' for i in range(600))
    assert logs.image_layer_progress(text)['total'] == 512


def test_missing_container_log_is_waiting_not_arbitrary_errors():
    """Only the provider's exact temporary missing-container response is suppressed."""
    from vast_provisioning_log import _missing_log
    assert _missing_log('Error response from daemon: No such container: C.50932336\n')
    assert not _missing_log('Error response from daemon: permission denied')
    assert not _missing_log('Worker failed\nError response from daemon: No such container: C.50932336')
