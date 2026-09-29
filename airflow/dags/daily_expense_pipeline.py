from datetime import datetime, timedelta
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path("/opt/project")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def generate_daily_expenses(**context):

    from producers.expense_batch_producer import (
        generate_expense_records,
    )

    records = generate_expense_records()

    print(
        f"Generated {len(records)} daily expense records."
    )

    print(
        f"Expense date: {records[0]['expense_date']}"
    )

    return records


def write_daily_expenses_to_csv(**context):

    from producers.expense_batch_producer import (
        write_csv,
    )

    records = context["ti"].xcom_pull(
        task_ids="generate_daily_expenses"
    )

    if not records:
        raise ValueError(
            "No expense records received from "
            "generate_daily_expenses."
        )

    write_csv(records)

    print(
        f"Wrote {len(records)} expense records to CSV."
    )


def publish_daily_expenses_to_kafka(**context):

    from producers.expense_batch_producer import (
        create_producer,
        publish_to_kafka,
    )

    records = context["ti"].xcom_pull(
        task_ids="generate_daily_expenses"
    )

    if not records:
        raise ValueError(
            "No expense records received from "
            "generate_daily_expenses."
        )

    producer = create_producer()

    try:
        publish_to_kafka(
            producer,
            records,
        )

        print(
            f"Published {len(records)} expense "
            f"records to Kafka."
        )

    finally:
        producer.close()


default_args = {
    "owner": "ride-hailing-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="daily_vehicle_expense_pipeline",
    description=(
        "Generate, persist and publish the daily "
        "vehicle expense batch."
    ),
    default_args=default_args,
    start_date=datetime(2026, 9, 28),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=[
        "ride-hailing",
        "batch",
        "kafka",
        "data-engineering",
    ],
) as dag:

    generate_expenses = PythonOperator(
        task_id="generate_daily_expenses",
        python_callable=generate_daily_expenses,
    )

    write_csv = PythonOperator(
        task_id="write_daily_expenses_to_csv",
        python_callable=write_daily_expenses_to_csv,
    )

    publish_kafka = PythonOperator(
        task_id="publish_daily_expenses_to_kafka",
        python_callable=publish_daily_expenses_to_kafka,
    )

    generate_expenses >> [
        write_csv,
        publish_kafka,
    ]