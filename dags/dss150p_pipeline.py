from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator


PROJECT = "/opt/airflow/project"


def failure_callback(context):
    """Print useful task/run information when a task fails."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    print("PIPELINE TASK FAILED")
    print(f"dag_id={dag_run.dag_id if dag_run else 'unknown'}")
    print(f"run_id={dag_run.run_id if dag_run else 'unknown'}")
    print(f"task_id={task_instance.task_id if task_instance else 'unknown'}")
    print(f"error={exception}")


DEFAULT_ARGS = {
    "owner": "dss150p",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": failure_callback,
}


with DAG(
    dag_id="dss150p_sales_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=30),
    params={
        "run_mode": Param(
            "full",
            enum=["full", "partition"],
        ),
        "year": Param(
            2026,
            type="integer",
            minimum=2000,
            maximum=2100,
        ),
        "month": Param(
            1,
            type="integer",
            minimum=1,
            maximum=12,
        ),
    },
    tags=["DSS150P"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=(
            f"cd {PROJECT} && "
            'PIPELINE_RUN_ID="{{ run_id }}" '
            "python -m src.cli extract"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=(
            f"cd {PROJECT} && "
            'PIPELINE_RUN_ID="{{ run_id }}" '
            "python -m src.cli transform"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    load = BashOperator(
        task_id="load",
        bash_command=(
            f"cd {PROJECT} && PIPELINE_RUN_ID=\"{{{{ run_id }}}}\" "
            "{% if params.run_mode == 'partition' %}"
            "python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}"
            "{% else %}"
            "python -m src.cli load"
            "{% endif %}"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=(
            f"cd {PROJECT} && "
            'PIPELINE_RUN_ID="{{ run_id }}" '
            "python -m src.cli validate"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    extract >> transform >> load >> validate