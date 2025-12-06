from airflow.models import BaseOperator
from sqlalchemy import text

# Absolute import – this works in Airflow 3.x
from airflow_provider_iris.hooks.iris_hook import IrisHook


class IrisSQLOperator(BaseOperator):
    template_fields = ("sql",)

    def __init__(self, sql, iris_conn_id="iris_default", autocommit=True, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.iris_conn_id = iris_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        hook = IrisHook(iris_conn_id=self.iris_conn_id)
        engine = hook.get_engine()

        with engine.connect() as conn:
            result = conn.execute(text(self.sql))

            if self.sql.strip().upper().startswith(("SELECT", "WITH")):
                rows = result.fetchall()
                self.log.info(f"Query returned {len(rows)} rows → {rows[:10]}")

            if self.autocommit:
                conn.commit()

        engine.dispose()