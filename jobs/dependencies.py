"""Скачать два официальных модуля Iceberg и проверить SHA-512 из Maven Central."""
import hashlib
import urllib.request
from client import ROOT

directory = ROOT / '.cache/jars'
directory.mkdir(parents=True,exist_ok=True)
for artifact in ('iceberg-spark-runtime-3.5_2.12','iceberg-aws-bundle'):
    name = f'{artifact}-1.8.1.jar'
    url = f'https://repo.maven.apache.org/maven2/org/apache/iceberg/{artifact}/1.8.1/{name}'
    with urllib.request.urlopen(url + '.sha512', timeout=60) as response:
        checksum = response.read().decode().split()[0]
    path = directory / name
    if not path.exists() or hashlib.sha512(path.read_bytes()).hexdigest() != checksum:
        with urllib.request.urlopen(url, timeout=120) as response, path.open('wb') as output:
            while chunk := response.read(1024*1024):
                output.write(chunk)
    assert hashlib.sha512(path.read_bytes()).hexdigest() == checksum, name
    print(name + ': SHA-512 проверен',flush=True)
