
from __future__ import annotations

import glob as globmod
import os

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DAG_ID = "customer_ingestion_dag"
BEAM_JAR = "/opt/airflow/beam/Beam-1.0.jar"
ALLOWED_TYPES = {"CSV", "JSON"}


def _path_present(path: str) -> bool:
    if os.path.isfile(path) or os.path.isdir(path):
        return True
    return ("*" in path or "?" in path) and bool(globmod.glob(path))


def _validate_file(file_path: str, file_type: str, execution_id: str, **_context) -> str:
    if file_type.upper() not in ALLOWED_TYPES:
        raise ValueError(f"file_type must be one of {sorted(ALLOWED_TYPES)}, got {file_type!r}")
    if not execution_id or not execution_id.strip():
        raise ValueError("execution_id must be a non-empty UUID string")
    if not _path_present(file_path):
        raise FileNotFoundError(
            f"file_path not found inside the Airflow container: {file_path!r}. "
            "Hint: the API must send the CONTAINER path (e.g. /opt/airflow/data/customers.csv), "
            "not your laptop path. For split runs, the directory /opt/airflow/split/<execution_id>/"
            " must exist (did the API's PySpark split run?)"
        )
    return f"validated {file_type} file {file_path} for execution {execution_id}"


with DAG(
    dag_id=DAG_ID,
    description="Ingest one SOR feed into MySQL via Beam (Lumi case study, Phases 1-3)",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    params={
        "file_path": Param("/opt/airflow/data/customers.csv", type="string"),
        "file_type": Param("CSV", type="string", enum=sorted(ALLOWED_TYPES)),
        "execution_id": Param("manual-test-run", type="string"),
        "error_output": Param("/opt/airflow/errors/errors", type="string"),
        "control_file": Param("", type="string"),
    },
) as dag:

    validate_file = PythonOperator(
        task_id="validate_file",
        python_callable=_validate_file,
        op_kwargs={
            "file_path": "{{ params.file_path }}",
            "file_type": "{{ params.file_type }}",
            "execution_id": "{{ params.execution_id }}",
        },
    )

    run_beam_ingestion = BashOperator(
        task_id="run_beam_ingestion",
        bash_command=(
            f"java -jar {BEAM_JAR} "
            '--filePath="{{ params.file_path }}" '
            '--fileType="{{ params.file_type }}" '
            '--executionId="{{ params.execution_id }}" '
            '--errorOutput="{{ params.error_output }}" '
            '--controlFile="{{ params.control_file }}"'
        ),
        append_env=True,
    )

    validate_file >> run_beam_ingestion