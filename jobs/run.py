import argparse
import json
import math
import statistics
import subprocess
from client import ROOT, RESULTS, Trino, json_request, q10, statements, save_json, save_csv

SCALE = 'sf2'
TABLES = ('customer', 'orders', 'lineitem', 'nation')
CASES = [
    ('bronze', 'PARQUET', 'ZSTD'),
    ('bronze_gzip', 'PARQUET', 'GZIP'),
    ('bronze_orc_zstd', 'ORC', 'ZSTD'),
    ('bronze_orc_gzip', 'ORC', 'GZIP'),
]


def docker(*args):
    r = subprocess.run(['docker', *args], cwd=ROOT, check=True,
                       capture_output=True, text=True, encoding='utf-8')
    return r.stdout


def connection():
    db = Trino()
    for key, value in [('task_concurrency', 4), ('task_min_writer_count', 1), ('task_max_writer_count', 1)]:
        db.query(f'SET SESSION {key} = {value}')
    return db


def check_q10(rows, expected):
    actual = {r[0]: r for r in rows}
    reference = {r[0]: r for r in expected}
    assert len(rows) == len(actual) == len(reference) == 20
    assert actual.keys() == reference.keys()
    errors = []
    for key, row in actual.items():
        other = reference[key]
        assert row[:2] + row[3:] == other[:2] + other[3:], key
        assert math.isclose(row[2], other[2], rel_tol=1e-9, abs_tol=1e-6), key
        errors.append(abs(row[2] - other[2]))
    return max(errors)


def setup():
    docker('compose', 'exec', '-T', 'minio', 'mc', 'alias', 'set', 'lab',
           'http://localhost:9000', 'labstore', 'bulaeva-storage-local')
    docker('compose', 'exec', '-T', 'minio', 'mc', 'mb', '--ignore-existing', 'lab/lakehouse')
    base = 'http://localhost:28181/management/v1'
    if not json_request(base + '/info').get('bootstrapped'):
        json_request(base + '/bootstrap', {'accept-terms-of-use': True})
    warehouses = json_request(base + '/warehouse?project-id=00000000-0000-0000-0000-000000000000')
    found = [w for w in warehouses.get('warehouses', []) if w['name'] == 'returns']
    if found:
        warehouse = found[0]
    else:
        warehouse = json_request(base + '/warehouse', {
            'warehouse-name': 'returns', 'project-id': '00000000-0000-0000-0000-000000000000',
            'storage-profile': {'type': 's3', 'bucket': 'lakehouse', 'key-prefix': 'returns',
                                'endpoint': 'http://minio:9000', 'region': 'us-east-1',
                                'path-style-access': True, 'flavor': 's3-compat', 'sts-enabled': False},
            'storage-credential': {'type': 's3', 'credential-type': 'access-key',
                                   'access-key-id': 'labstore', 'secret-access-key': 'bulaeva-storage-local'},
        })
    save_json('catalog.json', warehouse)
    print('Каталог returns готов.', flush=True)


def benchmark(overwrite=False):
    db = connection()
    existing = {r[0] for r in db.query('SHOW SCHEMAS FROM lake')['rows']}
    if not overwrite and any(schema in existing for schema, _, _ in CASES):
        raise SystemExit('Для повторной записи добавьте --overwrite.')
    for schema, _, _ in CASES:
        db.query(f'CREATE SCHEMA IF NOT EXISTS lake.{schema}')
    expected = db.query(q10(f'tpch.{SCALE}'), 'source_q10')
    save_json('source_q10.json', expected)
    writes = []
    for repeat in range(3):
        for schema, fmt, codec in CASES[repeat:] + CASES[:repeat]:
            db.query(f"SET SESSION lake.compression_codec = '{codec}'")
            total = 0
            for table, sql in zip(TABLES, statements('bronze.sql', schema=schema, format=fmt, scale=SCALE)):
                result = db.query(sql.replace('CREATE TABLE', 'CREATE OR REPLACE TABLE'), f'load_{schema}_{table}_{repeat+1}')
                seconds = result['stats']['elapsedTimeMillis'] / 1000
                total += seconds
                sizes = db.query(f'SELECT sum(record_count), sum(file_size_in_bytes), count(*) FROM lake.{schema}."{table}$files"')['rows'][0]
                writes.append({'schema': schema, 'round': repeat+1, 'table': table,
                               'seconds': seconds, 'rows': sizes[0], 'bytes': sizes[1],
                               'files': sizes[2], 'query_id': result['query_id']})
                save_csv('load_times.csv', writes)
            print(f'{schema}: запись {repeat+1}, {total:.3f} с', flush=True)
    runs, warmups = [], []
    for schema, _, _ in CASES:
        result = db.query(q10(f'lake.{schema}'), f'warmup_{schema}')
        error = check_q10(result['rows'], expected['rows'])
        warmups.append({'schema': schema, 'seconds': result['stats']['elapsedTimeMillis']/1000,
                        'max_error': error, 'query_id': result['query_id']})
    save_csv('warmup.csv', warmups)
    for repeat in range(5):
        offset = repeat % len(CASES)
        for schema, _, _ in CASES[offset:] + CASES[:offset]:
            result = db.query(q10(f'lake.{schema}'), f'q10_{schema}_{repeat+1}')
            error = check_q10(result['rows'], expected['rows'])
            runs.append({'schema': schema, 'round': repeat+1,
                         'seconds': result['stats']['elapsedTimeMillis']/1000,
                         'cpu_seconds': result['stats']['cpuTimeMillis']/1000,
                         'read_bytes': result['stats'].get('physicalInputBytes', 0),
                         'max_error': error, 'query_id': result['query_id']})
            save_csv('query_times.csv', runs)
            print(f'{schema}: Q10 {repeat+1}, {runs[-1]["seconds"]:.3f} с', flush=True)
    summary = []
    for schema, fmt, codec in CASES:
        timings = [r['seconds'] for r in runs if r['schema'] == schema]
        loads = [sum(r['seconds'] for r in writes if r['schema'] == schema and r['round'] == n) for n in (1,2,3)]
        size = sum(r['bytes'] for r in writes if r['schema'] == schema and r['round'] == 3)
        summary.append({'schema': schema, 'format': fmt, 'compression': codec, 'bytes': size,
                        'mib': size/1048576, 'write_s': statistics.median(loads),
                        'q10_s': statistics.median(timings), 'min_s': min(timings), 'max_s': max(timings)})
    save_csv('comparison.csv', summary)
    print('Сравнение форматов готово.', flush=True)


def build():
    db = connection()
    db.query("SET SESSION lake.compression_codec = 'ZSTD'")
    counts = {}
    for table in TABLES:
        counts[table] = db.scalar(f'SELECT count(*) FROM lake.bronze.{table}')
        assert counts[table] == db.scalar(f'SELECT count(*) FROM tpch.{SCALE}.{table}')
    checks = {}
    for table, key in [('customer','custkey'),('orders','orderkey'),('nation','nationkey')]:
        checks[table + '_duplicate_keys'] = db.scalar(f'SELECT count(*) - count(DISTINCT {key}) FROM lake.bronze.{table}')
    sql_checks = {
        'invalid_positions': 'SELECT count(*) FROM lake.bronze.lineitem WHERE orderkey IS NULL OR extendedprice IS NULL OR extendedprice < 0 OR discount IS NULL OR discount < 0 OR discount > 1 OR returnflag IS NULL',
        'unknown_customers': 'SELECT count(*) FROM lake.bronze.orders o LEFT JOIN lake.bronze.customer c ON c.custkey=o.custkey WHERE c.custkey IS NULL',
        'unknown_orders': 'SELECT count(*) FROM lake.bronze.lineitem l LEFT JOIN lake.bronze.orders o ON o.orderkey=l.orderkey WHERE o.orderkey IS NULL',
        'unknown_nations': 'SELECT count(*) FROM lake.bronze.customer c LEFT JOIN lake.bronze.nation n ON n.nationkey=c.nationkey WHERE n.nationkey IS NULL',
    }
    for label, sql in sql_checks.items():
        checks[label] = db.scalar(sql)
    assert all(n == 0 for n in checks.values()), checks
    for filename in ('prepare.sql', 'mart.sql'):
        for sql in statements(filename):
            db.query(sql, filename)
    detail_count = db.scalar("SELECT count(*) FROM lake.bronze.orders o JOIN lake.bronze.lineitem l ON l.orderkey=o.orderkey WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01' AND l.returnflag='R'")
    summary_count = db.scalar('SELECT sum(returned_positions) FROM lake.silver.order_returns')
    assert detail_count == summary_count
    assert db.scalar('SELECT count(*) - count(DISTINCT orderkey) FROM lake.silver.order_returns') == 0
    gold = db.query('SELECT * FROM lake.gold.customer_refunds ORDER BY revenue DESC')
    source = json.loads((RESULTS/'source_q10.json').read_text(encoding='utf-8'))['rows']
    check_q10(gold['rows'], source)
    save_csv('top20.csv', [dict(zip(gold['columns'], row)) for row in gold['rows']])
    # Новая колонка в Silver без перезаписи данных.
    files_sql = 'SELECT file_path, file_size_in_bytes FROM lake.silver."order_returns$files" ORDER BY file_path'
    before = db.query(files_sql)['rows']
    db.query('ALTER TABLE lake.silver.order_returns ADD COLUMN review_note VARCHAR')
    db.query('ALTER TABLE lake.silver.order_returns RENAME COLUMN review_note TO analyst_note')
    assert before == db.query(files_sql)['rows']
    nulls = db.scalar('SELECT count(*) FROM lake.silver.order_returns WHERE analyst_note IS NULL')
    assert nulls == db.scalar('SELECT count(*) FROM lake.silver.order_returns')
    # Восстановление суммы после ошибочного обновления.
    table = 'lake.gold.customer_refunds'
    snapshot = db.scalar('SELECT snapshot_id FROM lake.gold."customer_refunds$snapshots" ORDER BY committed_at DESC LIMIT 1')
    key, original = gold['rows'][0][0], gold['rows'][0][2]
    update = f'UPDATE {table} SET revenue = 0 WHERE custkey = {key}'
    historical = f'SELECT revenue FROM {table} FOR VERSION AS OF {snapshot} WHERE custkey = {key}'
    rollback = f'ALTER TABLE {table} EXECUTE rollback_to_snapshot({snapshot})'
    db.query(update)
    changed = db.scalar(f'SELECT revenue FROM {table} WHERE custkey = {key}')
    old = db.scalar(historical)
    assert changed == 0 and old == original
    db.query(rollback)
    restored = db.query(f'SELECT * FROM {table} ORDER BY revenue DESC')
    check_q10(restored['rows'], source)
    (RESULTS/'restore.sql').write_text(';\n\n'.join([update,historical,rollback])+';\n',encoding='utf-8')
    print('Silver, Gold, изменение схемы и откат проверены.', flush=True)


def inspect():
    db = Trino()
    wid = json.loads((RESULTS/'catalog.json').read_text(encoding='utf-8'))['warehouse-id']
    tables = [(s,t) for s,_,_ in CASES for t in TABLES]
    tables += [('silver','customer_directory'),('silver','order_returns'),('gold','customer_refunds')]
    inventory = []
    for schema, table in tables:
        meta = json_request(f'http://localhost:28181/catalog/v1/{wid}/namespaces/{schema}/tables/{table}')['metadata']
        fields = next(s['fields'] for s in meta['schemas'] if s['schema-id'] == meta['current-schema-id'])
        files = db.query(f'SELECT file_size_in_bytes, file_format FROM lake.{schema}."{table}$files"')['rows']
        target = 'lab/' + meta['location'].removeprefix('s3://')
        objects = [json.loads(line) for line in docker('compose','exec','-T','minio','mc','ls','--recursive','--json',target).splitlines()]
        assert all(o.get('status') == 'success' for o in objects)
        inventory.append({'schema':schema,'table':table,'rows':db.scalar(f'SELECT count(*) FROM lake.{schema}.{table}'),
                          'columns':', '.join(f['name'] for f in fields), 'files':len(files),
                          'format':files[0][1], 'active_bytes':sum(f[0] for f in files),
                          'metadata_bytes':sum(o.get('size',0) for o in objects if '/metadata/' in '/'+o.get('key','')),
                          'all_bytes':sum(o.get('size',0) for o in objects),
                          'partitions':json.dumps(meta['partition-specs']), 'location':meta['location']})
    save_csv('storage.csv', inventory)
    print('Размеры таблиц сохранены.', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('step', choices=['setup','bench','build','inspect'])
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()
    if args.step == 'bench':
        benchmark(args.overwrite)
    else:
        {'setup': setup, 'build': build, 'inspect': inspect}[args.step]()
