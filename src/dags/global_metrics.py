from lib import PgConnect
import vertica_python
from vertica_python import Connection
import logging
from typing import Dict
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator 
from airflow.operators.empty import EmptyOperator
import pendulum

logger = logging.getLogger(__name__)

class GlobalMetricRepository:
    def __init__(self, vertica: Connection) -> None:
        self._db = vertica

    def load_global_metric(self) -> None:
        try:
            with self._db.cursor() as cur:
                merge_query = """
                    MERGE INTO VT260113606F4C__DWH.global_metrics AS g
                    USING (
                         SELECT
                            DATE(transaction_dt) AS date_update,
                            t.currency_code AS currency_from,
                            SUM(CASE
                                WHEN t.currency_code = 430 THEN t.amount
                                ELSE t.amount / c.currency_with_div
                            END) AS amount_total,
                            COUNT(DISTINCT t.operation_id) AS cnt_transactions,
                            COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions,
                            COUNT(DISTINCT t.operation_id) * 1.0 / NULLIF(COUNT(DISTINCT t.account_number_from), 0) AS avg_transactions_per_account
                        FROM VT260113606F4C__STAGING.transactions t
                        JOIN VT260113606F4C__STAGING.currencies c ON t.currency_code = c.currency_code
                        WHERE t.account_number_from >= 0 AND t.status = 'done'
                        GROUP BY DATE(transaction_dt), t.currency_code 
                    ) AS src
                    ON g.date_update = src.date_update AND g.currency_from = src.currency_from
                    WHEN MATCHED THEN
                        UPDATE SET 
                            amount_total = src.amount_total,
                            cnt_transactions = src.cnt_transactions,
                            avg_transactions_per_account = src.avg_transactions_per_account,
                            cnt_accounts_make_transactions = src.cnt_accounts_make_transactions
                    WHEN NOT MATCHED THEN
                        INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
                        VALUES (src.date_update, src.currency_from, src.amount_total, src.cnt_transactions, src.avg_transactions_per_account, src.cnt_accounts_make_transactions);
                """
                cur.execute(merge_query)

            self._db.commit()
            logger.info("Данные успешно загружены в VT260113606F4C__DWH.global_metrics")

        except Exception as e:
            self._db.rollback()
            logger.error(f"Ошибка при загрузке данных в Vertica: {e}")
            raise


class GlobalMetricLoader:
    def __init__(self, vertica_config: Dict) -> None:
        self.vertica_config = vertica_config

        try:
            self.vertica_conn = vertica_python.connect(**self.vertica_config)
            logger.info("Соединение с Vertica установлено.")
        except Exception as e:
            logger.error(f"Ошибка подключения к Vertica: {e}")
            raise
        self.repository = GlobalMetricRepository(self.vertica_conn)

    def load_report(self):
        self.repository.load_global_metric()


@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 10, 31, tz="UTC"),
    catchup=True,
    is_paused_upon_creation=True,
) 
def global_metrics_report():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    vertica_config = {
        "host": "vertica.data-engineer.education-services.ru",
        "port": 5433,
        "user": "vt260113606f4c",
        "password": "857b173c9e284cad8d56077922be148d",
    }

    def global_metrics_load(execution_date: str):
        rest_loader = GlobalMetricLoader(vertica_config)
        rest_loader.load_report()

    global_metrics_load_task = PythonOperator(
        task_id="load_global_metric",
        python_callable=global_metrics_load,
        op_kwargs={"execution_date": "2022-10-01"},
    )

    start >> global_metrics_load_task >> end

_ = global_metrics_report()
