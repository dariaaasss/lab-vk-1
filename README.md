# Возвраты клиентов, Q10

Булаева Дарья. Практическая работа 1, вариант 2.

Данные TPC-H загружаются в Bronze. В Silver возвраты объединяются по заказам, затем из них строится топ-20 клиентов в Gold. В отчёте сравниваются Parquet и ORC с GZIP и ZSTD на масштабе `sf2`.

## Как запустить

Понадобятся Docker Compose с Linux-контейнерами и Python 3.10+. Для Docker использовалось 7,6 ГиБ памяти. Под образы и данные стоит оставить около 15 ГиБ на диске.

Для хранения файлов на D:

```powershell
New-Item -ItemType Directory -Force D:\lab_vk_data\bulaeva
Copy-Item .env.example .env
```

Другую папку можно указать в `WAREHOUSE_PATH`. Без `.env` используется Docker-том.

Из папки проекта:

```powershell
./run.ps1
```

Путь к Python при необходимости передаётся через `-Python 'C:\path\to\python.exe'`.

Отдельные этапы:

```powershell
./run.ps1 -Step setup
./run.ps1 -Step bench
./run.ps1 -Step build
./run.ps1 -Step spark
./run.ps1 -Step inspect
```

Для повторной записи таблиц: `./run.ps1 -Overwrite`. Замеры в CSV заменяются новыми, журнал SQL дополняется.

## Что находится в папке

- [report.md](report.md): отчёт с решениями и результатами.
- [compose.yaml](compose.yaml): S3-хранилище, каталог, Trino и Spark.
- `sql/`: исходный Q10 и SQL для трёх слоёв.
- `jobs/`: загрузка, замеры, проверки и подключение Spark.
- `results/`: числа по каждому запуску, итоговые таблицы и вывод Spark.

## Доступ к стенду

Trino доступен на [localhost:28080](http://localhost:28080), пользователь `bulaeva`. S3-консоль находится на [localhost:29001](http://localhost:29001), логин `labstore`, пароль `bulaeva-storage-local`. Lakekeeper доступен на [localhost:28181](http://localhost:28181/ui/).

Прочитать итоговую таблицу:

```powershell
docker compose exec trino trino --execute 'SELECT * FROM lake.gold.customer_refunds ORDER BY revenue DESC'
```

Остановка: `docker compose stop`. Повторный запуск с сохранёнными данными: `docker compose start`.

## Документация

- [Trino 476, Iceberg](https://github.com/trinodb/trino/blob/476/docs/src/main/sphinx/connector/iceberg.md)
- [Iceberg 1.8.1, Spark](https://iceberg.apache.org/docs/1.8.1/spark-configuration/)
