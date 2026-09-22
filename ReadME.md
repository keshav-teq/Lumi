# Lumi — Batch Customer Data Ingestion Pipeline

Lumi, a data ingestion pipeline for raw customer files (CSV / JSON): files are chunked with **Spark**, validated, obfuscated and loaded with **Apache Beam**, orchestrated by **Airflow**, and triggered through a **Spring Boot REST API**. Everything lands in **MySQL**, ready for analysis - with per-run error quarantine so one bad row never kills a load.

## The flow

```
 raw customer files
        |
        v
+------------------+      files over 1 MiB are pre-split
|  Spring Boot API | ---> by PySpark into 2000-line chunks
+--------+---------+
         |  REST trigger: executionId + file path
         v
+------------------+      one DAG run per file/chunk
|     Airflow      | ---> runs the Beam job as a task
+--------+---------+
         |  java -jar
         v
+------------------+      parse -> validate -> obfuscate
|   Apache Beam    | ---> batch load to MySQL (JdbcIO)
+--------+---------+      rejects -> error file, per run
         v
      MySQL (lumi)
```

## Tech stack

| Layer | Technology | Version |
|---|---|---|
| Processing core | Apache Beam (DirectRunner) | 2.58.x |
| File splitting | PySpark | 3.5.x |
| Orchestration | Apache Airflow (Docker Compose) | 2.9.x |
| Trigger service | Spring Boot REST API | 3.3.x |
| Database | MySQL | 8.x |
| Language / build | Java 17, Python 3.10, Maven | |

## What each layer does

**Apache Beam job:**
- Reads any supported format and normalizes to a `Customer` record.
- **Validates** every row (ID pattern `C######`, email shape, dates, numeric amounts, required fields).
- **Obfuscates** sensitive fields (phone numbers, annual spend) with a reversible `b64:` marker *after* validation, so raw formats never reach the database.
- Batch-writes valid rows to MySQL via `JdbcIO`.
- Writes invalid rows to an error file — one per run and source file — instead of failing the run.
- Optional control file records per-run row counts for audit.

**PySpark splitter** — big files become parallel work: if a file exceeds 1 MiB, the splitter breaks it into fixed-line chunks (2000 lines each), preserving source order and extension, so every chunk becomes an independent, individually rerunnable ingestion.

**Airflow** — orchestration and operational hygiene: a single parameterized DAG takes `file_path`, `file_type`, `execution_id`; retries, scheduling, task logs and the UI come for free. Runs entirely in Docker.

**Spring Boot API** — accepts a file reference, decide split-or-not, trigger the DAG over Airflow's REST endpoint, and report the execution ID for tracking.

**MySQL schema** — one target table (`warehouse/schema.sql`), with obfuscated columns clearly named so downstream consumers know what is analysis-safe.

## Configuration

The Beam job reads its database settings **only from environment variables**:

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `DB_URL` | no | `jdbc:mysql://localhost:3306/lumi` | JDBC URL, in containers use `host.docker.internal` |
| `DB_USERNAME` | **yes** | — | fail-fast if missing |
| `DB_PASSWORD` | **yes** | — | fail-fast if missing |

Set them per runtime context: IDE run configuration for host runs, `docker-compose.yml` `environment:` for Airflow.

## Getting started

```bash
# 1. Database: create schema and a user which handles database operations.
mysql -u temp -p < warehouse/schema.sql

# 2. Build the Beam JAR File.
cd beam-job && mvn package && cd ..

# 3. Airflow: Handles orchestration.
cd airflow && docker compose up -d
# UI: http://localhost:8080

# 4. Trigger a Data Ingestion Request through Spring API from POSTMAN/CURL Request.
curl -X POST http://localhost:8081/api/v1/ingestions \
  -H 'Content-Type: application/json' \
  -d '{"fileLocation": "C:/abs/path/to/sample-data/customers.csv", "fileType": "CSV"}'

## Sample data

`sample-data/` holds small, synthetic fixtures for each supported format — including deliberately invalid rows, so an end-to-end run visibly produces both loaded rows and quarantined errors.
```

## Project layout

```
|-- Beam/               Apache Beam ingestion
|-- Ingest/             Spring Boot trigger service
|-- Airflow/    
    |-- .airflow/       DAG, Dockerfile, docker-compose.yml 
    |-- beam-job/       Beam JAR File
    |-- sample-data/    Sample Data Files
    |-- split-output    Directory which stores the splitted files.
|-- PySpark/            PySpark file splitter
|-- Warehouse/          SQL Schema file
```