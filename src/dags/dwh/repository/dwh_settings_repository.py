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
    schema = "VT260113606F4C__DWH"
    
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        logger.info(conn,"Соединение с Vertica работае.")
        with conn.cursor() as cur:
            
            cur.execute(
                """
                SELECT date, workflow_key, workflow_settings
                FROM "VT260113606F4C__DWH"."srv_wf_settings"
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
                    updated_at = :updated_at
                WHERE workflow_key = :workflow_key;
            """
            cursor.execute(update_query, {
                "workflow_key": workflow_key,
                "workflow_settings": json.dumps(workflow_settings),
                "updated_at": datetime.now().isoformat(),
            })

            insert_query = f"""
                INSERT INTO {self.schema}.srv_wf_settings
                    (workflow_key, workflow_settings, updated_at)
                SELECT
                    :workflow_key,
                    :workflow_settings,
                    :updated_at
                WHERE NOT EXISTS (
                    SELECT 1 FROM {self.schema}.srv_wf_settings
                    WHERE workflow_key = :workflow_key
                );
            """
            cursor.execute(insert_query, {
                "workflow_key": workflow_key,
                "workflow_settings": json.dumps(workflow_settings),
                "updated_at": datetime.now().isoformat(),
            })

            conn.commit()
            cursor.close()
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка при сохранении настроек в Vertica: {e}")
            raise

