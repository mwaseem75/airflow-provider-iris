# File: plugins/airflow_provider_intersystems_iris/hooks/iris_hook.py

from airflow.hooks.base import BaseHook
from typing import Any
from sqlalchemy import create_engine

class IrisHook(BaseHook):
    conn_name_attr = "iris_conn_id"
    default_conn_name = "iris_default"
    conn_type = "iris"
    hook_name = "InterSystems IRIS"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": [],
            "relabeling": {"login": "Username", "schema": "Namespace"},
            "placeholders": {
                "port": "1972",
            },
        }

    def __init__(self, iris_conn_id: str = "iris_default"):
        super().__init__()
        self.iris_conn_id = iris_conn_id

    def get_engine(self):
        # This will use the Airflow connection you created (iris_default)
        conn = self.get_connection(self.iris_conn_id)

        # Build the SQLAlchemy-IRIS connection URL
        url = f"iris://{conn.login}:{conn.password}@{conn.host}:{conn.port or 1972}/{conn.schema or 'USER'}"

        return create_engine(url, future=True, echo=False)