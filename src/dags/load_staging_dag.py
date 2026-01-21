import contextlib
from datetime import datetime
from typing import List, Optional
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from lib import ConnectionBuilder
import vertica_python
from vertica_python import Connection as VerticaConnection
from psycopg import Connection as PGConnection
from pydantic import BaseModel
from lib import PgConnect
from repository import EtlSetting, DwhEtlSettingsRepository
import pendulum
from datetime import timedelta

# Настройка логгера
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

class CurrencyPgObj(BaseModel):
    date_update: datetime
    currency_code: int
    currency_code_with: int
    currency_with_div: float

class TransactionPgObj(BaseModel):
    operation_id: str
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: int
    transaction_dt: datetime

class CurrencyRawRepository:
    def load_raw_currencies(
        self, pg_conn: PGConnection, ds_date: str, last_loaded_date: Optional[datetime]
    ) -> List[CurrencyPgObj]:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    date_update,
                    currency_code,
                    currency_code_with,
                    currency_with_div
                FROM public.currencies
                WHERE date_update > %(last_loaded_date)s AND date(date_update) = %(ds_date)s;
                """,
                {"last_loaded_date": last_loaded_date,
                "ds_date": ds_date },
            )
            rows = cur.fetchall()
        return [CurrencyPgObj(**{ 
            "date_update": row[0],
            "currency_code": row[1],
            "currency_code_with": row[2],
            "currency_with_div": row[3]
        }) for row in rows]
    
class TransactionRawRepository:
    def load_raw_transactions(
        self, pg_conn: PGConnection, ds_date: str, last_loaded_date: Optional[datetime]
    ) -> List[TransactionPgObj]:
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                FROM public.transactions
                WHERE transaction_dt > %(last_loaded_date)s AND date(transaction_dt) = %(ds_date)s;
                """,
                {"last_loaded_date": last_loaded_date,
                "ds_date": ds_date },
            )
            rows = cur.fetchall()
        return [TransactionPgObj(**{ 
            "operation_id": row[0],
            "account_number_from": row[1],
            "account_number_to": row[2],
            "currency_code": row[3],
            "country": row[4],
            "status": row[5],
            "transaction_type": row[6],
            "amount": row[7],
            "transaction_dt": row[8]
        }) for row in rows]

class CurrencyLoader:
    WF_KEY = "currencies_raw_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_date"
    VERTICA_SCHEMA = "VT260113606F4C__STAGING"

    def __init__(self, pg: PgConnect, vertica_config: dict) -> None:
        self.dwh = pg
        self.raw = CurrencyRawRepository()
        self.settings_repository = DwhEtlSettingsRepository()
        self.vertica_config = vertica_config

        try:
            self.vertica_conn = vertica_python.connect(**self.vertica_config)
            logger.info("Соединение с Vertica установлено.")
        except Exception as e:
            logger.error(f"Ошибка подключения к Vertica: {e}")
            raise

    def __del__(self):
        if self.vertica_conn and not self.vertica_conn.closed():
            self.vertica_conn.close()
            logger.info("Соединение с Vertica закрыто.")

    def connect_to_pg(self) -> PGConnection:
        return self.dwh.connection()
    
    def load_currencies(self, ds_date: str):
        try:
            wf_setting = self.settings_repository.get_setting(
                self.vertica_conn, self.WF_KEY 
            )
            if not wf_setting:
                wf_setting = EtlSetting(
                    date=datetime.min,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: None}
                )

            last_loaded_date = wf_setting.workflow_settings.get(self.LAST_LOADED_ID_KEY)
            if last_loaded_date is None:
                last_loaded_date = datetime.min

            with self.connect_to_pg() as pg_conn:
                load_queue = self.raw.load_raw_currencies(pg_conn, ds_date, last_loaded_date)
                load_queue.sort(key=lambda x: x.date_update)

            if not load_queue:
                logger.info("Нет новых данных для загрузки.")
                return

            for currency in load_queue:
                self.insert_currency_vertica(currency, wf_setting)

            latest_date = load_queue[-1].date_update
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = latest_date.isoformat()

            self.settings_repository.save_setting(
                self.vertica_conn,
                wf_setting.workflow_key,
                wf_setting.workflow_settings
            )
            logger.info(f"Загружено {len(load_queue)} записей. Последняя дата: {latest_date}")

        except Exception as e:
            logger.error(f"Ошибка в load_currencies: {e}")
            raise

    def insert_currency_vertica(self, currency: CurrencyPgObj, wf_setting: EtlSetting) -> None:
        try: 
            cursor = self.vertica_conn.cursor()

            merge_query = f"""
                MERGE INTO {self.VERTICA_SCHEMA}.currencies AS target
                USING (SELECT 
                    TO_TIMESTAMP(:date_update, 'YYYY-MM-DD HH24:MI:SS') AS date_update,
                    :currency_code AS currency_code,
                    :currency_code_with AS currency_code_with,
                    :currency_with_div AS currency_with_div) AS source
                ON target.currency_code = source.currency_code
                AND target.date_update = source.date_update
                AND target.currency_code_with = source.currency_code_with
                WHEN MATCHED THEN
                    UPDATE SET currency_with_div = source.currency_with_div
                WHEN NOT MATCHED THEN
                    INSERT (date_update, currency_code, currency_code_with, currency_with_div)
                    VALUES (source.date_update, source.currency_code, source.currency_code_with, source.currency_with_div);
            """

            cursor.execute(merge_query, {
                "date_update": currency.date_update.isoformat(),  # Убедитесь, что передаете строку в нужном формате
                "currency_code": currency.currency_code,
                "currency_code_with": currency.currency_code_with,
                "currency_with_div": currency.currency_with_div,
            })

            self.vertica_conn.commit()
            cursor.close()
        except Exception as e:
            self.vertica_conn.rollback()
            logger.error(f"Ошибка при вставке в Vertica: {e}")
            raise



class TransactionLoader:
    WF_KEY = "transactions_raw_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_date"
    VERTICA_SCHEMA = "VT260113606F4C__STAGING"

    def __init__(self, pg: PgConnect, vertica_config: dict) -> None:
        self.dwh = pg
        self.raw = TransactionRawRepository()
        self.settings_repository = DwhEtlSettingsRepository()
        self.vertica_config = vertica_config

        try:
            self.vertica_conn = vertica_python.connect(**self.vertica_config)
            logger.info("Соединение с Vertica установлено.")
        except Exception as e:
            logger.error(f"Ошибка подключения к Vertica: {e}")
            raise

    def __del__(self):
        if self.vertica_conn and not self.vertica_conn.closed():
            self.vertica_conn.close()
            logger.info("Соединение с Vertica закрыто.")

    def connect_to_pg(self) -> PGConnection:
        return self.dwh.connection()
    
    def load_transactions(self, ds_date: str):
        try:
            wf_setting = self.settings_repository.get_setting(
                self.vertica_conn, self.WF_KEY 
            )
            if not wf_setting:
                wf_setting = EtlSetting(
                    date=datetime.min,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: None}
                )

            last_loaded_date = wf_setting.workflow_settings.get(self.LAST_LOADED_ID_KEY)
            if last_loaded_date is None:
                last_loaded_date = datetime.min

            with self.connect_to_pg() as pg_conn:
                batch_size = 10000  # Размер пакета
                while True:
                    load_queue = self.raw.load_raw_transactions(pg_conn, ds_date, last_loaded_date)
                    load_queue.sort(key=lambda x: x.transaction_dt)

                    if not load_queue:
                        logger.info("Нет новых данных для загрузки.")
                        break

                    # Разделение на батчи
                    for i in range(0, len(load_queue), batch_size):
                        batch = load_queue[i:i + batch_size]
                        for transaction in batch:
                            logger.info(f"Начало")
                            self.insert_transaction_vertica(transaction, wf_setting)
                        logger.info(f"Середина")
                        last_loaded_date = batch[-1].transaction_dt
                        wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded_date.isoformat()
                        self.settings_repository.save_setting(
                            self.vertica_conn,
                            wf_setting.workflow_key,
                            wf_setting.workflow_settings
                        )

                        logger.info(f"Загружено {len(batch)} транзакций. Последняя дата: {last_loaded_date}")

        except Exception as e:
            logger.error(f"Ошибка в load_transactions: {e}")
            raise


    def insert_transaction_vertica(self, transaction: TransactionPgObj, wf_setting: EtlSetting) -> None:
        try:
            cursor = self.vertica_conn.cursor()

            merge_query = f"""
                MERGE INTO {self.VERTICA_SCHEMA}.transactions AS target
                USING (SELECT 
                    :operation_id AS operation_id,
                    :account_number_from AS account_number_from,
                    :account_number_to AS account_number_to,
                    :currency_code AS currency_code,
                    :country AS country,
                    :status AS status,
                    :transaction_type AS transaction_type,
                    :amount AS amount,
                    TO_TIMESTAMP(:transaction_dt, 'YYYY-MM-DD"T"HH24:MI:SS') AS transaction_dt ) AS source
                ON target.operation_id = source.operation_id
                AND target.transaction_dt = source.transaction_dt
                AND target.status = source.status
                WHEN MATCHED THEN
                    UPDATE SET 
                        account_number_from = source.account_number_from,
                        account_number_to = source.account_number_to,
                        currency_code = source.currency_code,
                        country = source.country,
                        transaction_type = source.transaction_type,
                        amount = source.amount
                WHEN NOT MATCHED THEN
                    INSERT (operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
                    VALUES (source.operation_id, source.account_number_from, source.account_number_to, source.currency_code, source.country, source.status, source.transaction_type, source.amount, source.transaction_dt);
            """

            cursor.execute(merge_query, {
                "operation_id": transaction.operation_id,
                "account_number_from": transaction.account_number_from,
                "account_number_to": transaction.account_number_to,
                "currency_code": transaction.currency_code,
                "country": transaction.country,
                "status": transaction.status,
                "transaction_type": transaction.transaction_type,
                "amount": transaction.amount,
                "transaction_dt": transaction.transaction_dt.isoformat(), 
            })

            self.vertica_conn.commit()
            cursor.close()
        except Exception as e:
            self.vertica_conn.rollback()
            logger.error(f"Ошибка при вставке в Vertica: {e}")
            raise


@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 10, 31, tz="UTC"),
    catchup=True,
    is_paused_upon_creation=True,
)
def dag_load_data_to_staging(): 
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_CONNECTION")

    vertica_config = {
        "host": "vertica.data-engineer.education-services.ru",
        "port": 5433,
        "user": "vt260113606f4c",
        "password": "857b173c9e284cad8d56077922be148d",
    }

    def load_currencies(execution_date: str):
        execution_datetime = pendulum.parse(execution_date) 
        rest_loader = CurrencyLoader(dwh_pg_connect, vertica_config)
        rest_loader.load_currencies(execution_date)

    def load_transactions(execution_date: str):
        execution_datetime = pendulum.parse(execution_date)
        rest_loader = TransactionLoader(dwh_pg_connect, vertica_config)
        rest_loader.load_transactions(execution_date)

    load_currencies_task = PythonOperator(
        task_id="load_currencies",
        python_callable=load_currencies,
        # op_kwargs={"execution_date": "2022-10-01"},
        op_kwargs={"execution_date": "{{ macros.ds_add(ds, -111) }}"} ,
    )

    load_transactions_task = PythonOperator(
        task_id="load_transactions",
        python_callable=load_transactions,
        # op_kwargs={"execution_date": "2022-10-01"} ,
        op_kwargs={"execution_date": "{{ macros.ds_add(ds, -111) }}"} ,
        execution_timeout=timedelta(minutes=30) 
    )

    start >> [load_currencies_task, load_transactions_task] >> end

_ = dag_load_data_to_staging()
