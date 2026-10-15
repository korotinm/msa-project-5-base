import csv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_PATH = "/opt/airflow/data/delivery_status.csv"
FAILED_THRESHOLD = 5.0  # % недоставленных, после которого идём в ветку проверки
EMAIL_TO = "marketing@example.com"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
}


def read_deliveries(ti):
    with open(CSV_PATH) as f:
        rows = list(csv.DictReader(f))
    failed = sum(1 for r in rows if r["status"] == "failed")
    ti.xcom_push(key="failed_share", value=round(100 * failed / len(rows), 2))


def read_orders(ti):
    # первая попытка падает специально, чтобы показать retry
    if get_current_context()["ti"].try_number < 2:
        raise RuntimeError("shop db временно недоступна")
    hook = PostgresHook(postgres_conn_id="shop_db")
    total, revenue = hook.get_first(
        "SELECT count(*), coalesce(sum(amount), 0) FROM orders WHERE status = 'paid'"
    )
    ti.xcom_push(key="orders_total", value=total)
    ti.xcom_push(key="revenue", value=float(revenue))


def choose_branch(ti):
    share = ti.xcom_pull(task_ids="read_deliveries", key="failed_share")
    return "check_logistics" if share > FAILED_THRESHOLD else "build_report"


def save_report(ti, ds):
    hook = PostgresHook(postgres_conn_id="shop_db")
    hook.run(
        """
        INSERT INTO delivery_report (run_date, orders_total, failed_share, revenue)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (run_date) DO UPDATE SET
            orders_total = EXCLUDED.orders_total,
            failed_share = EXCLUDED.failed_share,
            revenue = EXCLUDED.revenue
        """,
        parameters=(
            ds,
            ti.xcom_pull(task_ids="read_orders", key="orders_total"),
            ti.xcom_pull(task_ids="read_deliveries", key="failed_share"),
            ti.xcom_pull(task_ids="read_orders", key="revenue"),
        ),
    )


with DAG(
    "delivery_report",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:
    read_deliveries_t = PythonOperator(task_id="read_deliveries", python_callable=read_deliveries)
    read_orders_t = PythonOperator(task_id="read_orders", python_callable=read_orders)
    branch = BranchPythonOperator(task_id="choose_branch", python_callable=choose_branch)

    check_logistics = EmailOperator(
        task_id="check_logistics",
        to=EMAIL_TO,
        subject="Высокая доля недоставленных заказов",
        html_content="Доля недоставленных: {{ ti.xcom_pull(task_ids='read_deliveries', key='failed_share') }}%",
    )
    build_report = PythonOperator(task_id="build_report", python_callable=save_report)

    done = EmptyOperator(task_id="done", trigger_rule="none_failed_min_one_success")
    notify_ok = EmailOperator(
        task_id="notify_success",
        to=EMAIL_TO,
        subject="Пайплайн delivery_report завершён",
        html_content="Запуск {{ ds }} завершён успешно",
    )
    notify_fail = EmailOperator(
        task_id="notify_failure",
        to=EMAIL_TO,
        subject="Пайплайн delivery_report упал",
        html_content="Запуск {{ ds }} завершился с ошибкой",
        trigger_rule="one_failed",
    )

    [read_deliveries_t, read_orders_t] >> branch
    branch >> [check_logistics, build_report] >> done >> notify_ok
    [read_deliveries_t, read_orders_t, build_report] >> notify_fail
