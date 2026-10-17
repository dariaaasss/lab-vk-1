"""Чтение Bronze и проверка Q10 в Spark."""
import csv
import json
import math
from pathlib import Path
import time
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName('returns-check')
         .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
         .config('spark.sql.catalog.lake','org.apache.iceberg.spark.SparkCatalog')
         .config('spark.sql.catalog.lake.type','rest')
         .config('spark.sql.catalog.lake.uri','http://lakekeeper:8181/catalog')
         .config('spark.sql.catalog.lake.warehouse','returns')
         .config('spark.sql.catalog.lake.io-impl','org.apache.iceberg.aws.s3.S3FileIO')
         .config('spark.sql.catalog.lake.s3.endpoint','http://minio:9000')
         .config('spark.sql.catalog.lake.s3.path-style-access','true')
         .config('spark.sql.catalog.lake.s3.access-key-id','labstore')
         .config('spark.sql.catalog.lake.s3.secret-access-key','bulaeva-storage-local')
         .config('spark.sql.catalog.lake.client.region','us-east-1')
         .config('spark.sql.shuffle.partitions','4')
         .getOrCreate())
spark.sparkContext.setLogLevel('WARN')
out = Path('/opt/work/results')
counts = {t:spark.table(f'lake.bronze.{t}').count() for t in ['customer','orders','lineitem','nation']}
with (out/'load_times.csv').open(newline='', encoding='utf-8') as f:
    expected_counts = {row['table']: int(row['rows']) for row in csv.DictReader(f)
                       if row['schema'] == 'bronze' and row['round'] == '3'}
assert counts == expected_counts
spark.sql('SELECT * FROM lake.bronze.customer ORDER BY custkey LIMIT 5').show(truncate=False)
query = Path('/opt/work/sql/q10.sql').read_text().format(source='lake.bronze')
started = time.perf_counter()
rows = [list(r) for r in spark.sql(query).collect()]
seconds = time.perf_counter()-started
reference = json.loads((out/'source_q10.json').read_text())['rows']
assert len(rows)==len(reference)==20
for a,e in zip(sorted(rows),sorted(reference)):
    assert a[:2]+a[3:]==e[:2]+e[3:]
    assert math.isclose(a[2],e[2],rel_tol=1e-9,abs_tol=1e-6)
print('SPARK_BRONZE_OK',json.dumps(counts),f'Q10: {seconds:.3f} s, 20 rows')

# Проверка кодека в физическом файле nation каждого варианта Bronze.
# Файлы маленькие; чтение выполняется после всех измерений производительности.
jvm = spark._jvm
fileio = jvm.org.apache.iceberg.aws.s3.S3FileIO()
options = jvm.java.util.HashMap()
for k,v in {'s3.endpoint':'http://minio:9000','s3.path-style-access':'true',
            's3.access-key-id':'labstore','s3.secret-access-key':'bulaeva-storage-local',
            'client.region':'us-east-1'}.items():
    options.put(k,v)
fileio.initialize(options)
codecs = []
for schema,fmt,expected in [('bronze_gzip','PARQUET','GZIP'),
                             ('bronze','PARQUET','ZSTD'),
                             ('bronze_orc_gzip','ORC','ZLIB'),
                             ('bronze_orc_zstd','ORC','ZSTD')]:
    path = spark.sql(f'SELECT file_path FROM lake.{schema}.nation.files LIMIT 1').first()[0]
    stream = fileio.newInputFile(path).newStream()
    local = Path(f'/tmp/p1-{schema}.{fmt.lower()}')
    try:
        local.write_bytes(bytes(stream.readAllBytes()))
    finally:
        stream.close()
    jpath = jvm.org.apache.hadoop.fs.Path(str(local))
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    if fmt == 'PARQUET':
        footer = jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(conf,jpath)
        observed = footer.getBlocks().get(0).getColumns().get(0).getCodec().toString()
    else:
        reader = jvm.org.apache.orc.OrcFile.createReader(jpath,jvm.org.apache.orc.OrcFile.readerOptions(conf))
        observed = reader.getCompressionKind().toString()
        reader.close()
    assert observed == expected, (schema,observed,expected)
    codecs.append({'schema':schema,'table':'nation','format':fmt,'compression':observed,'file':path})
fileio.close()
print('FILE_CODECS_OK',json.dumps(codecs))
spark.stop()
