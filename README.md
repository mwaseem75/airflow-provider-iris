# airflow-provider-iris
<img width="630" alt="image" src="https://raw.githubusercontent.com/mwaseem75/streamlitLLM/main/images/airflowlogo.png" />

[![one](https://img.shields.io/badge/Platform-InterSystems%20IRIS-blue)](https://www.intersystems.com/data-platform/)
[![one](https://img.shields.io/badge/Workflow%20Orchestration-Aoache%20Airflow-success)](https://airflow.apache.org/)
[![one](https://img.shields.io/badge/PyPI%20Package-airflow%20provider%20iris-yellowgreen)](https://pypi.org/project/airflow-provider-iris/)
[![one](https://img.shields.io/badge/PyPI%20Availabe%20on-Open%20Exchange-blue)](https:///)
[![License](https://img.shields.io/badge/License-Apache%202.0-00b2a9.svg)](https://opensource.org/licenses/Apache-2.0)
---
## Table of Contents

- Overview
- Installation
- Quick Start
- IrisSQLOperator Parameters
- Examples
  - 01_IRIS_Raw_SQL_Demo
  - 02_IRIS_ORM_Demo
  - 03_IRIS_Load_Synthetic_Data_Demo

---
<a name="overview"></a>
## Overview
InterSystems IRIS Provider for Apache Airflow enables seamless integration between Airflow workflows and the InterSystems IRIS data platform. It provides native connection support and operators for executing IRIS SQL and automating IRIS-driven tasks within modern ETL/ELT pipelines.

Designed for reliability and ease of use, this provider helps data engineers and developers build scalable, production-ready workflows for healthcare, interoperability, analytics, and enterprise data processing—powered by InterSystems IRIS.

### Features
* ✔️ `IrisHook` – for managing IRIS connections
* ✔️ `IrisSQLOperator` – for running SQL queries
* ✔️ Support for both SELECT/CTE and DML statements
* ✔️ Native Airflow connection UI customization
* ✔️ Examples for real-world ETL patterns

---
<a name="installation"></a>
## Installation

```bash
pip install airflow-provider-iris
```
<a name="quick-start"></a>
## Quick Start

Configure Connection in Airflow UI
Go to Admin → Connections → Add Connection
* Conn Id: **Connection ID**
* Description : **Connection Description**
* Conn Type: **InterSystems IRIS**
* Host: **IRIS server hostname**
* Username: **User Name**
* Password: **Password**
* Port : **IRIS Superserver Port**
* Namespace : **Namespace**
<img width="1127" alt="image" src="https://raw.githubusercontent.com/mwaseem75/streamlitLLM/main/images/connection.png" />

Use your InterSystems IRIS connection by setting the `iris_conn_id` parameter in any of the provided operators.

In the example below, the `IrisSQLOperator` uses the `iris_conn_id` parameter to connect to the IRIS instance when the DAG is defined: 
```python
from airflow_provider_iris.operators.iris_operator import IrisSQLOperator

with DAG(
    dag_id="01_IRIS_Raw_SQL_Demo_Local",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["iris-contest"],
) as dag:
    
    create_table = IrisSQLOperator(
        task_id="create_table",
        iris_conn_id="ContainerInstance",
        sql="""CREATE TABLE IF NOT EXISTS Test.AirflowDemo (
               ID INTEGER IDENTITY PRIMARY KEY,
               Message VARCHAR(200),
               RunDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
    )
```
<a name="connector-parameters"></a>
## IrisSQLOperator Parameters

| Parameter       | Description                                                            | Type / Default         | Required | 
|-----------------|------------------------------------------------------------------------|------------------------|----------|
| **sql**         | SQL query or template                                                  | `str`                  | Yes      |
| **iris_conn_id**| IRIS connection identifier                                             | `str` / `iris_default` | Yes      |
| **task_id**     | DAG task name                                                          | `str`                  | Yes      |
| **autocommit**  | Commit changes automatically                                           | `bool` / `True`        | No       |
| ****kwargs**    | Airflow BaseOperator arguments                                         | --                     | No       |


## Examples
### 1. IRIS Raw SQL Demo
Usage of RAW SQL statements
```python
# dags/01_IRIS_Raw_SQL_Demo.py
from datetime import datetime
from airflow import DAG
from airflow_provider_iris.operators.iris_operator import IrisSQLOperator

# ---------------------------------------------------------------------
# Example DAG showing how to run raw SQL statements on InterSystems IRIS
# using the IrisSQLOperator included in the provider.
# ---------------------------------------------------------------------

with DAG(
    dag_id="01_IRIS_Raw_SQL_Demo",
    start_date=datetime(2025, 12, 1),
    schedule=None,        # Run manually – no recurring schedule
    catchup=False,
    tags=["iris-contest"],
) as dag:
    
    # Create a simple table to store demo entries.
    # IF NOT EXISTS ensures the DAG can be re-run without errors.
    create_table = IrisSQLOperator(
        task_id="create_table",
        sql="""
            CREATE TABLE IF NOT EXISTS AirflowDemo.Test (
                ID INTEGER IDENTITY PRIMARY KEY,
                Message VARCHAR(200),
                RunDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
    )

    # Insert a sample row into the table.
    # This demonstrates a basic INSERT operation using the operator.
    insert = IrisSQLOperator(
        task_id="insert_row",
        sql="""
            INSERT INTO AirflowDemo.Test (Message)
            VALUES ('Hello from raw SQL operator')
        """,
    )

    # Retrieve all rows so the results appear in the Airflow logs.
    # Useful to confirm end-to-end connectivity with IRIS.
    select = IrisSQLOperator(
        task_id="select_rows",
        sql="""
            SELECT ID, Message, RunDate
            FROM AirflowDemo.Test
            ORDER BY ID DESC
        """,
    )

    # Task order: create table → insert row → select rows
    create_table >> insert >> select
```
<a name="2-iris-orm-demo"></a>
### 2. IRIS ORM Demo
Uses SQLAlchemy + pandas with the only known reliable method for bulk inserts into IRIS.
```
# dags/02_IRIS_ORM_Demo.py

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np

# Import IRIS hook and SQLAlchemy components
from airflow_provider_iris.hooks.iris_hook import IrisHook
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# ---------------------------------------------------------------------
# ORM MODEL
# Defines the structure of the AirflowDemo.ORMSales table in IRIS.
# ---------------------------------------------------------------------
class SalesRecord(Base):
    __tablename__ = "ORMSales"
    __table_args__ = {"schema": "AirflowDemo"}

    id        = Column(Integer, primary_key=True)
    region    = Column(String(50))
    amount    = Column(Float)
    sale_date = Column(DateTime)


# ---------------------------------------------------------------------
# TASK 1: Create table and insert synthetic sample records.
# Uses pandas.to_sql() with chunksize=1 → most consistent for IRIS.
# ---------------------------------------------------------------------
def create_and_insert_orm(**context):
    # If you use a non-default connection → ALWAYS pass iris_conn_id explicitly
    # e.g hook = IrisHook(iris_conn_id="iris_Connection_ID")
    hook = IrisHook()
    engine = hook.get_engine()

    # Ensure the table exists
    Base.metadata.create_all(engine)

    # ---- Generate synthetic generic sample data ----
    num_records = 5
    regions = [f"Region {i}" for i in range(1, num_records + 1)]

    sample_data = [
        {
            "region": region,
            "amount": round(np.random.uniform(5000, 50000), 2),
            "sale_date": pd.Timestamp("2025-12-01") + pd.Timedelta(days=i)
        }
        for i, region in enumerate(regions)
    ]

    df = pd.DataFrame(sample_data)

    # Insert rows → IRIS requires single-batch inserts for reliability
    df.to_sql(
        name="ORMSales",
        con=engine,
        schema="AirflowDemo",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1,   # key setting for IRIS compatibility
    )

    print(f"Inserted {len(df)} generated rows into AirflowDemo.ORMSales")


# ---------------------------------------------------------------------
# TASK 2: Query back data and print rows in Airflow logs.
# ---------------------------------------------------------------------
def query_orm(**context):
    hook = IrisHook()
    engine = hook.get_engine()

    df = pd.read_sql(
        "SELECT * FROM AirflowDemo.ORMSales ORDER BY id",
        engine
    )

    for _, r in df.iterrows():
        print(
            f"ORM → {int(r.id):>3} | "
            f"{r.region:<15} | "
            f"${r.amount:>10,.2f} | "
            f"{r.sale_date.date()}"
        )


# ---------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------
with DAG(
    dag_id="02_IRIS_ORM_Demo",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["iris-contest", "orm"],
) as dag:

    orm_create = PythonOperator(
        task_id="orm_create_and_insert",
        python_callable=create_and_insert_orm,
    )

    orm_read = PythonOperator(
        task_id="orm_read",
        python_callable=query_orm,
    )

    orm_create >> orm_read
```
<a name="3-synthetic-sales-pipeline"></a>
### 3. Synthetic Sales Pipeline
Generate realistic sales data and load efficiently.

```
# dags/03_IRIS_Load_Synthetic_Data_Demo

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from airflow_provider_iris.hooks.iris_hook import IrisHook
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# ---------------------------------------------------------------------
# ORM model representing a generic sales table in the "AirflowDemo" schema.
# Reusable for both table creation and data insertion.
# ---------------------------------------------------------------------
class SalesRecord(Base):
    __tablename__ = "BulkSales"
    __table_args__ = {"schema": "AirflowDemo"}

    id        = Column(Integer, primary_key=True)
    region    = Column(String(50))
    amount    = Column(Float)
    sale_date = Column(DateTime)


# ---------------------------------------------------------------------
# Generate synthetic sales data for testing or demo purposes.
# Supports dynamic number of rows and produces realistic random data.
# ---------------------------------------------------------------------
def generate_synthetic_sales(num_rows=500):
    """Create synthetic sales data as a pandas DataFrame."""
    
    regions = [
        "North America", "South America", "Europe",
        "Asia-Pacific", "Middle East", "Africa"
    ]

    # Randomly pick a region for each row
    region_data = np.random.choice(regions, size=num_rows)

    # Random sales amounts between 10k and 120k
    amounts = np.random.uniform(10000, 120000, size=num_rows).round(2)

    # Random sale dates within last 30 days
    start_date = datetime(2025, 11, 1)
    sale_dates = [
        start_date + timedelta(days=int(x)) 
        for x in np.random.randint(0, 30, size=num_rows)
    ]

    # Construct a DataFrame
    df = pd.DataFrame({
        "region": region_data,
        "amount": amounts,
        "sale_date": sale_dates
    })

    return df


# ---------------------------------------------------------------------
# Airflow task: bulk load synthetic sales data into IRIS.
# ---------------------------------------------------------------------
def bulk_load_synthetic_sales(**context):

    # Generate synthetic dataset
    df = generate_synthetic_sales(num_rows=200)

    # Create SQLAlchemy engine via IRIS hook
    # If you use a non-default connection → ALWAYS pass iris_conn_id explicitly
    # e.g hook = IrisHook(iris_conn_id="iris_Connection_ID")
    hook = IrisHook()
    engine = hook.get_engine()

    # Ensure table exists
    Base.metadata.create_all(engine)

    # Bulk insert into IRIS
    df.to_sql(
        "BulkSales",
        con=engine,
        schema="AirflowDemo",
        if_exists="append",
        index=False
    )

    print(f"Bulk loaded {len(df)} synthetic rows into AirflowDemo.BulkSales")


# ---------------------------------------------------------------------
# DAG definition
# Demonstrates ETL-style bulk load of synthetic sales data into IRIS.
# ---------------------------------------------------------------------
with DAG(
    dag_id="03_IRIS_Load_Synthetic_Data_Demo",
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    tags=["iris-contest", "etl", "synthetic"],
) as dag:

    bulk_task = PythonOperator(
        task_id="bulk_load_synthetic_sales",
        python_callable=bulk_load_synthetic_sales
    )

```


    
