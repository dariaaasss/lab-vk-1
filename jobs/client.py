"""HTTP-клиент Trino и сохранение результатов. Только стандартная библиотека."""
import csv
import datetime as dt
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'results'
RESULTS.mkdir(exist_ok=True)
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def json_request(url, payload=None, method=None):
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, body, {'Content-Type': 'application/json'}, method=method)
    try:
        with OPENER.open(req, timeout=60) as r:
            data = r.read()
            return json.loads(data) if data else None
    except urllib.error.HTTPError as e:
        raise RuntimeError(f'{e.code} {url}: {e.read().decode()}') from e


def save_json(name, data):
    (RESULTS / name).write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def save_csv(name, rows, fields=None):
    with (RESULTS / name).open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields or list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class Trino:
    def __init__(self):
        self.base = os.environ.get('TRINO_URL', 'http://localhost:28080')
        self.session = {}

    def query(self, sql, label='query'):
        sql = sql.strip().rstrip(';')
        headers = {'X-Trino-User': 'bulaeva', 'X-Trino-Source': 'bulaeva-q10',
                   'X-Trino-Time-Zone': 'UTC', 'Content-Type': 'text/plain; charset=utf-8'}
        if self.session:
            headers['X-Trino-Session'] = ','.join(f'{k}={v}' for k, v in self.session.items())
        started = time.perf_counter()
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
        request = urllib.request.Request(self.base + '/v1/statement', sql.encode(), headers)
        rows, columns = [], []
        while True:
            with OPENER.open(request, timeout=120) as response:
                for setting in response.headers.get_all('X-Trino-Set-Session', []):
                    k, v = setting.split('=', 1)
                    self.session[k] = v
                data = json.load(response)
            columns = data.get('columns', columns)
            rows.extend(data.get('data', []))
            if 'error' in data or 'nextUri' not in data:
                break
            if time.perf_counter() - started > 1200:
                with OPENER.open(urllib.request.Request(data['nextUri'], method='DELETE')):
                    pass
                raise TimeoutError(f'Query {data["id"]} exceeded 20 minutes')
            request = urllib.request.Request(data['nextUri'], headers=headers)
        record = {'label': label, 'timestamp_utc': timestamp, 'query_id': data['id'],
                  'sql': sql, 'client_seconds': time.perf_counter() - started,
                  'stats': data['stats'], 'rows_returned': len(rows)}
        if 'error' in data:
            record['error'] = data['error']
        with (RESULTS / 'queries.jsonl').open('a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
        if 'error' in data:
            raise RuntimeError(f'{data["id"]}: {data["error"]["message"]}')
        record['rows'] = rows
        record['columns'] = [c['name'] for c in columns]
        return record

    def scalar(self, sql):
        return self.query(sql)['rows'][0][0]


def statements(name, **values):
    text = (ROOT / 'sql' / name).read_text(encoding='utf-8').format(**values)
    return [s.strip() for s in text.split(';') if s.strip()]


def q10(source):
    return (ROOT / 'sql/q10.sql').read_text(encoding='utf-8').format(source=source)
