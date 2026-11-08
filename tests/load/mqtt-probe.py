import base64
import concurrent.futures
import hashlib
import json
import random
import secrets
import signal
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.request
import uuid
from pathlib import Path

import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parents[2]
BINARY = ROOT / 'target/release/iot-server'


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def new_client(device_id, token, port, on_connect):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=device_id,
                         clean_session=True, protocol=mqtt.MQTTv311,
                         reconnect_on_failure=False)
    client.username_pw_set(device_id, token)
    client.on_connect = on_connect
    client.connect('127.0.0.1', port, keepalive=10)
    return client


def connect_attempt(device_id, token, port, barrier=None):
    result = []
    if barrier:
        barrier.wait(timeout=5)
    client = new_client(device_id, token, port,
                        lambda _c, _u, _f, rc, _p: result.append(str(rc)))
    deadline = time.monotonic() + 5
    while not result and time.monotonic() < deadline:
        client.loop(timeout=0.05)
    client.disconnect()
    return result[0] if result else 'timeout'


def retry_connect(device_id, token, port, initial_reason):
    attempts = []
    if initial_reason != 'Server unavailable':
        raise RuntimeError(f'expected initial server unavailable, got {initial_reason}')
    for attempt in range(8):
        time.sleep(random.uniform(0, min(0.05 * 2 ** attempt, 0.5)))
        reason = connect_attempt(device_id, token, port)
        attempts.append(reason)
        if reason == 'Success':
            return attempts
        if reason not in ('Server unavailable', 'Server unavailable or busy'):
            raise RuntimeError(f'unexpected CONNACK: {reason}')
    raise RuntimeError('bounded reconnect exhausted')


def telemetry_retry(device_id, token, port, event_id):
    connected, subscribed, acks, pubacks, discarded = [], [], [], [], []
    client = new_client(device_id, token, port,
                        lambda _c, _u, _f, rc, _p: connected.append(str(rc)))
    client.on_subscribe = lambda _c, _u, _m, codes, _p: subscribed.extend(codes)
    client.on_publish = lambda _c, _u, mid, _r, _p: pubacks.append(mid)
    def on_message(_client, _userdata, message):
        ack = json.loads(message.payload)
        if not discarded:
            discarded.append(ack)
        else:
            acks.append(ack)
    client.on_message = on_message

    def pump(predicate, seconds=5):
        deadline = time.monotonic() + seconds
        while not predicate() and time.monotonic() < deadline:
            client.loop(timeout=0.05)
        if not predicate():
            raise RuntimeError('MQTT operation timed out')

    pump(lambda: bool(connected))
    if connected != ['Success']:
        raise RuntimeError(f'telemetry connect failed: {connected}')
    client.subscribe(f'devices/{device_id}/telemetry/ack', qos=1)
    pump(lambda: bool(subscribed))
    if any(code.is_failure for code in subscribed):
        raise RuntimeError('telemetry ACK subscription denied')
    payload = json.dumps({'event_id': event_id, 'values': {'value': 1}},
                         separators=(',', ':')).encode()
    topic = f'devices/{device_id}/telemetry'
    first = client.publish(topic, payload, qos=1)
    pump(lambda: len(discarded) >= 1 and first.mid in pubacks)
    if discarded[0].get('event_id') != event_id or discarded[0].get('status') != 'stored':
        raise RuntimeError('first telemetry not stored')
    # Application ignores the first ACK and retries the unchanged event after timeout.
    deadline = time.monotonic() + 0.25
    while time.monotonic() < deadline:
        client.loop(timeout=0.01)
    if acks:
        raise RuntimeError('unexpected application ACK before retry')
    second = client.publish(topic, payload, qos=1)
    pump(lambda: len(acks) >= 1 and second.mid in pubacks)
    if acks[0].get('event_id') != event_id or acks[0].get('status') != 'duplicate':
        raise RuntimeError('identical retry not deduplicated')
    client.disconnect()
    return {'publish_requests': 2, 'pubacks': len(pubacks),
            'discarded_status': discarded[0]['status'],
            'application_ack_statuses': [ack['status'] for ack in acks], 'retries': 1}


def main():
    temp_root = '/tmp/opencode' if Path('/tmp/opencode').is_dir() else None
    with tempfile.TemporaryDirectory(prefix='iot-mqtt-probe-', dir=temp_root) as tmp:
        root = Path(tmp)
        http, port = free_port(), free_port()
        while port == http:
            port = free_port()
        config = root / 'conf.toml'
        config.write_text(f'config_version = 1\n[server]\ndata_dir = "./data"\n'
                          f'[http]\nlisten = "127.0.0.1:{http}"\n'
                          f'[mqtt]\nlisten = "127.0.0.1:{port}"\n'
                          '[database]\nurl = "sqlite://./data/iot.db"\n')
        subprocess.run([str(BINARY), 'migrate', '--config', str(config)],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        device_id = str(uuid.uuid7())
        token = 'iot_' + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip('=')
        db = root / 'data/iot.db'
        now = int(time.time() * 1000)
        devices = [(device_id, token)] + [
            (str(uuid.uuid7()), 'iot_' + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip('='))
            for _ in range(15)
        ]
        with sqlite3.connect(db) as conn:
            for index, (did, secret) in enumerate(devices):
                conn.execute("INSERT INTO devices (id,name,metadata_json,enabled,created_at,updated_at) VALUES (?,?,'{}',1,?,?)", (did, f'probe-{index}', now, now))
                conn.execute("INSERT INTO device_credentials (id,device_id,token_hash,status,created_at) VALUES (?,?,?,'active',?)", (str(uuid.uuid7()), did, hashlib.sha256(secret.encode()).digest(), now))
        server = subprocess.Popen([str(BINARY), 'run', '--config', str(config)],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 15
            while True:
                try:
                    with urllib.request.urlopen(f'http://127.0.0.1:{http}/readyz', timeout=1) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                if server.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('isolated server failed to become ready')
                time.sleep(0.05)
            barrier = threading.Barrier(16)
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                futures = [executor.submit(connect_attempt, device_id, token, port, barrier) for _ in range(16)]
                reasons = [future.result(timeout=10) for future in futures]
            if 'Server unavailable' not in reasons:
                raise RuntimeError('no server-unavailable response observed')
            retries = retry_connect(device_id, token, port, 'Server unavailable')
            time.sleep(0.1)
            events = [(did, secret, 'probe-' + uuid.uuid4().hex) for did, secret in devices]
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(events)) as executor:
                futures = [executor.submit(telemetry_retry, did, secret, port, event) for did, secret, event in events]
                telemetry = [future.result(timeout=20) for future in futures]
            with sqlite3.connect(db) as conn:
                rows = conn.execute('SELECT COUNT(*) FROM telemetry_events').fetchone()[0]
            if rows != len(events):
                raise RuntimeError(f'unexpected row count: {rows}')
            print(json.dumps({'connect_attempts': len(reasons),
                              'connack_counts': {reason: reasons.count(reason) for reason in sorted(set(reasons))},
                              'initial_retry_reason': 'Server unavailable',
                              'retry_attempts': len(retries), 'retry_results': retries,
                              'telemetry_devices': len(events),
                              'publish_requests': sum(item['publish_requests'] for item in telemetry),
                              'pubacks': sum(item['pubacks'] for item in telemetry),
                              'duplicates': sum(item['application_ack_statuses'].count('duplicate') for item in telemetry),
                              'discarded_stored': sum(item['discarded_status'] == 'stored' for item in telemetry),
                              'retries': sum(item['retries'] for item in telemetry), 'committed_rows': rows}))
        finally:
            server.send_signal(signal.SIGTERM)
            try:
                server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()


if __name__ == '__main__':
    main()
