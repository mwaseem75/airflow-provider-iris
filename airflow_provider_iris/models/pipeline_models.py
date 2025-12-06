# from sqlalchemy import Column, Integer, String, DateTime, Float
# from sqlalchemy.orm import declarative_base

# Base = declarative_base()

# class SalesRecord(Base):
#     __tablename__ = "SalesRecord"
#     __table_args__ = {"schema": "Test"}

#     id        = Column(Integer, primary_key=True)
#     region    = Column(String(50))
#     amount    = Column(Float)
#     sale_date = Column(DateTime)

#     # This combination works on ALL versions of SQLAlchemy-IRIS
#     __mapper_args__ = {
#         "eager_defaults": False,           # don't try to SELECT back defaults
#         "confirm_deleted_rows": False      # disables rowcount check on DELETE (bonus)
#     }

# plugins/airflow_provider_intersystems_iris/models/pipeline_models.py
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class SalesRecord(Base):
    __tablename__ = "SalesRecord"
    __table_args__ = {"schema": "Test"}

    id        = Column(Integer, primary_key=True)
    region    = Column(String(50))
    amount    = Column(Float)
    sale_date = Column(DateTime)