from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

REPO = "/opt/airflow/repo"
VENV_BASE = "/opt/airflow/.venvs"
PY_VENV = f"{VENV_BASE}/airflow_venv"
DBT_VENV = f"{VENV_BASE}/dbt_venv"

ENV_EXPORT = f"""
set -euxo pipefail
mkdir -p {VENV_BASE}
if [ -f {REPO}/.env ]; then
  set -a
  . <(sed 's/\\r$//' {REPO}/.env)
  set +a
fi
# If a docker-specific host is provided, prefer it inside the container
export PG_HOST="${{PG_HOST_DOCKER:-${{PG_HOST}}}}"
"""

default_args = {"owner": "ricky", "retries": 0}

with DAG(
    dag_id="retail_pipeline",
    description="Generate -> Load -> dbt run -> dbt test",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 1 * * *",
    catchup=False,
    default_args=default_args,
    tags=["retail","mvp"],
):
    generate = BashOperator(
        task_id="generate_data",
        bash_command=f"""
        {ENV_EXPORT}
        cd {REPO}

        if [ ! -d {PY_VENV} ]; then
          python -m venv {PY_VENV}
          . {PY_VENV}/bin/activate
          PIP_DISABLE_PIP_VERSION_CHECK=1 pip install -q \
            pandas==2.3.2 numpy==2.3.2 Faker==37.6.0 \
            psycopg2-binary==2.9.10 python-dotenv==1.1.1
        else
          . {PY_VENV}/bin/activate
        fi

        python -c "import pandas, numpy, faker; print('python deps ok')"
        python data/generate_batch.py
        ls -lh data/*.csv
        """
    )

    load = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"""
        {ENV_EXPORT}
        cd {REPO}
        . {PY_VENV}/bin/activate
        python load_to_postgres.py
        """
    )

    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"""
    {ENV_EXPORT}
    export DBT_PROFILES_DIR={REPO}/.dbt
    cd {REPO}/processing

    if [ ! -d {DBT_VENV} ]; then
      python -m venv {DBT_VENV}
      . {DBT_VENV}/bin/activate
      PIP_DISABLE_PIP_VERSION_CHECK=1 pip install -q dbt-postgres
    else
      . {DBT_VENV}/bin/activate
    fi

    # EITHER install git...
    # apt-get update -y >/dev/null 2>&1 && apt-get install -y -qq git >/dev/null 2>&1 || true
    # dbt debug

    # ...OR just don't fail on debug:
    dbt debug || echo "dbt debug non-blocking (git missing)"

    dbt run
    """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        {ENV_EXPORT}
        export DBT_PROFILES_DIR={REPO}/.dbt
        cd {REPO}/processing
        . {DBT_VENV}/bin/activate
        dbt test
        """
    )

    generate >> load >> dbt_run >> dbt_test
