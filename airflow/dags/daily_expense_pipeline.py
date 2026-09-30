import csv
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor


PROJECT_ROOT = Path("/opt/project")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


RECONCILE_SQL = (
    PROJECT_ROOT
    / "database"
    / "reconcile_profitability.sql"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "daily"
)

SIMULATED_DAY_MINUTES = float(
    os.getenv("SIMULATED_DAY_MINUTES", "5")
)

VEHICLE_COUNT = 25


def get_postgres_connection():

    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "ride_hailing"),
        user=os.getenv("POSTGRES_USER", "ride_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "ride_password"),
        connect_timeout=10,
    )


def get_report_date(context) -> str:

    records = context["ti"].xcom_pull(
        task_ids="generate_daily_expenses"
    )

    if not records:
        raise ValueError(
            "No expense records received from "
            "generate_daily_expenses."
        )

    return records[0]["expense_date"]


# ============================================================
# TASKS
# ============================================================

def generate_daily_expenses(**context):

    from producers.expense_batch_producer import (
        generate_expense_records,
    )
    from producers.simulation_clock import (
        SIMULATION_START_DATE,
        SimulationClock,
    )

    clock = SimulationClock()

    # During the first simulated day there is no completed day yet.
    # Skipping here also skips every downstream task of this run.
    if clock.previous_date() < SIMULATION_START_DATE.date():
        raise AirflowSkipException(
            f"No completed simulated day yet "
            f"(simulated now={clock.now().isoformat()})."
        )

    records = generate_expense_records(
        clock=clock
    )

    print(
        f"EXPENSE_BATCH_GENERATED "
        f"simulated_now={clock.now().isoformat()} "
        f"expense_date={records[0]['expense_date']} "
        f"records={len(records)}"
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

    csv_file = write_csv(records)

    print(
        f"EXPENSE_FILE_WRITTEN "
        f"path={csv_file} "
        f"records={len(records)}"
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
            f"EXPENSES_PUBLISHED "
            f"topic=vehicle_expenses "
            f"expense_date={records[0]['expense_date']} "
            f"records={len(records)}"
        )

    finally:
        producer.close()


def expenses_landed_in_postgres(**context) -> bool:


    report_date = get_report_date(context)

    conn = get_postgres_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM vehicle_expenses
                WHERE expense_date = %s::date;
                """,
                (report_date,),
            )

            landed = cursor.fetchone()[0]

    finally:
        conn.close()

    print(
        f"EXPENSES_LANDED_CHECK "
        f"expense_date={report_date} "
        f"landed={landed} "
        f"expected={VEHICLE_COUNT}"
    )

    return landed >= VEHICLE_COUNT


def reconcile_vehicle_profitability(**context):

    report_date = get_report_date(context)

    sql = RECONCILE_SQL.read_text(encoding="utf-8")

    conn = get_postgres_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                sql,
                {"report_date": report_date},
            )

            upserted = cursor.rowcount

            cursor.execute(
                """
                SELECT
                    profitability_status,
                    COUNT(*)
                FROM vehicle_profitability
                WHERE report_date = %s::date
                GROUP BY profitability_status
                ORDER BY profitability_status;
                """,
                (report_date,),
            )

            summary = dict(cursor.fetchall())

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    print(
        f"PROFITABILITY_RECONCILED "
        f"report_date={report_date} "
        f"rows={upserted} "
        f"summary={summary}"
    )


def export_daily_profitability_report(**context):

    report_date = get_report_date(context)

    conn = get_postgres_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    vehicle_id,
                    report_date,
                    ROUND(earnings::numeric, 2) AS earnings,
                    ROUND(fuel_cost::numeric, 2) AS fuel_cost,
                    ROUND(maintenance_cost::numeric, 2) AS maintenance_cost,
                    ROUND(operating_cost::numeric, 2) AS operating_cost,
                    ROUND(profit::numeric, 2) AS profit,
                    ROUND(profit_margin::numeric, 2) AS profit_margin,
                    ROUND(distance_covered::numeric, 2) AS distance_covered,
                    service_flag,
                    profitability_status
                FROM vehicle_profitability
                WHERE report_date = %s::date
                ORDER BY profit ASC;
                """,
                (report_date,),
            )

            rows = cursor.fetchall()
            columns = [
                column[0]
                for column in cursor.description
            ]

    finally:
        conn.close()

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_file = (
        REPORT_DIR
        / f"profitability_report_{report_date}.csv"
    )

    with report_file.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows(rows)

    unprofitable = [
        row[0]
        for row in rows
        if row[-1] == "UNPROFITABLE"
    ]

    print(
        f"PROFITABILITY_REPORT_EXPORTED "
        f"path={report_file} "
        f"vehicles={len(rows)} "
        f"unprofitable={unprofitable}"
    )


# ============================================================
# DAG
# ============================================================

default_args = {
    "owner": "ride-hailing-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


with DAG(
    dag_id="daily_vehicle_expense_pipeline",
    description=(
        "Generate, persist and publish the daily vehicle expense "
        "batch, then reconcile per-vehicle profitability."
    ),
    default_args=default_args,
    start_date=datetime(2026, 9, 28),
    # One run per simulated day.
    schedule=timedelta(minutes=SIMULATED_DAY_MINUTES),
    catchup=False,
    max_active_runs=1,
    tags=[
        "ride-hailing",
        "batch",
        "kafka",
        "profitability",
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

    wait_for_expenses = PythonSensor(
        task_id="wait_for_expenses_in_postgres",
        python_callable=expenses_landed_in_postgres,
        poke_interval=5,
        timeout=120,
        mode="poke",
    )

    reconcile = PythonOperator(
        task_id="reconcile_vehicle_profitability",
        python_callable=reconcile_vehicle_profitability,
    )

    export_report = PythonOperator(
        task_id="export_daily_profitability_report",
        python_callable=export_daily_profitability_report,
    )

    generate_expenses >> [
        write_csv,
        publish_kafka,
    ]

    publish_kafka >> wait_for_expenses >> reconcile >> export_report