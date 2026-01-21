from typing import Dict, Optional
import vertica_python 
from pydantic import BaseModel
from datetime import datetime
import json
import logging

from vertica_python import Connection
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EtlSetting(BaseModel):
    date: datetime
    workflow_key: str
    workflow_settings: Dict

class DwhEtlSettingsRepository:
    schema = "VT260113606F4C__STAGING"

    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor() as cur:
            
            cur.execute(
                """
                SELECT date, workflow_key, workflow_settings
                FROM "VT260113606F4C__STAGING"."srv_wf_settings"
                WHERE workflow_key = :etl_key;
                """,
                {"etl_key": etl_key}
            )
            row = cur.fetchone()

        if row:
            workflow_settings_dict = json.loads(row[2]) if isinstance(row[2], str) else row[2]
            return EtlSetting(
                date=row[0],
                workflow_key=row[1],
                workflow_settings=workflow_settings_dict
            )
        return None

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: dict) -> None:
        try:
            cursor = conn.cursor()

            update_query = f"""
                UPDATE {self.schema}.srv_wf_settings
                SET workflow_settings = :workflow_settings,
                    date = :date
                WHERE workflow_key = :workflow_key;
            """
            cursor.execute(update_query, {
                "workflow_key": workflow_key,
                "workflow_settings": json.dumps(workflow_settings),
                "date": workflow_settings.get('last_loaded_date', datetime.now().isoformat()), 
            })

            insert_query = f"""
                INSERT INTO {self.schema}.srv_wf_settings
                    (workflow_key, workflow_settings, date)
                SELECT
                    :workflow_key,
                    :workflow_settings,
                    :date
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.schema}.srv_wf_settings
                    WHERE workflow_key = :workflow_key
                );
            """
            cursor.execute(insert_query, {
                "workflow_key": workflow_key,
                "workflow_settings": json.dumps(workflow_settings),
                "date": workflow_settings.get('last_loaded_date', datetime.now().isoformat()), 
            })

            conn.commit()
            cursor.close()
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при сохранении настроек в Vertica: {e}")
            raise

