============================================================
InterSystems IRIS Provider for Apache Airflow
============================================================

.. image:: https://img.shields.io/pypi/v/airflow-provider-iris.svg
   :target: https://pypi.org/project/airflow-provider-iris/
   :alt: PyPI version

.. image:: https://img.shields.io/pypi/pyversions/airflow-provider-iris.svg
   :alt: Python versions

.. image:: https://img.shields.io/badge/Platform-InterSystems%20IRIS-blue
   :target: https://www.intersystems.com/data-platform/

.. image:: https://img.shields.io/badge/Workflow%20Orchestration-Apache%20Airflow-success
   :target: https://airflow.apache.org/

.. image:: https://img.shields.io/badge/License-Apache%202.0-00b2a9.svg
   :target: https://opensource.org/licenses/Apache-2.0

.. image:: https://raw.githubusercontent.com/mwaseem75/streamlitLLM/main/images/airflowlogo.png
   :width: 630
   :alt: Airflow + IRIS logo
   :align: center

.. contents:: Table of Contents
   :depth: 3
   :backlinks: top
   :local:

Overview
========

The **InterSystems IRIS Provider for Apache Airflow** enables seamless integration between Airflow workflows and the InterSystems IRIS data platform. It provides native connection support and operators for executing IRIS SQL and automating IRIS-driven tasks within modern ETL/ELT pipelines.

Designed for reliability and ease of use, this provider helps data engineers and developers build scalable, production-ready workflows for healthcare, interoperability, analytics, and enterprise data processing — powered by InterSystems IRIS.

Features
--------

- ``IrisHook`` – for managing IRIS connections
- ``IrisSQLOperator`` – for running SQL queries
- Support for both SELECT/CTE and DML statements
- Native Airflow connection UI customization
- Real-world ETL example DAGs included

Installation
============

.. code-block:: bash

    pip install airflow-provider-iris

Quick Start
===========

1. Configure Connection in Airflow UI
   Admin → Connections → Create

   - **Conn Id**: your choice (e.g., ``iris_default``)
   - **Conn Type**: ``InterSystems IRIS``
   - **Host**, **Port**, **Username**, **Password**, **Namespace**

   .. image:: https://raw.githubusercontent.com/mwaseem75/streamlitLLM/main/images/connection.png
      :width: 100%
      :alt: Airflow IRIS connection configuration

2. Use the operator in your DAG

.. code-block:: python

    from airflow_provider_iris.operators.iris_operator import IrisSQLOperator
    from datetime import datetime
    from airflow import DAG

    with DAG(
        dag_id="01_IRIS_Raw_SQL_Demo_Local",
        start_date=datetime(2025, 12, 1),
        schedule=None,
        catchup=False,
        tags=["iris-contest"],
    ) as dag:

        create_table = IrisSQLOperator(
            task_id="create_table",
            iris_conn_id="ContainerInstance",  # or your conn_id
            sql="""
            CREATE TABLE IF NOT EXISTS Test.AirflowDemo (
                ID INTEGER IDENTITY PRIMARY KEY,
                Message VARCHAR(200),
                RunDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
        )

Connector Parameters
====================

.. list-table::
   :widths: 20 45 20 15
   :header-rows: 1

   * - Parameter
     - Description
     - Type / Default
     - Required
   * - **sql**
     - SQL query or template to execute
     - ``str``
     - Yes
   * - **iris_conn_id**
     - Airflow connection ID for IRIS
     - ``str`` / ``iris_default``
     - Yes
   * - **task_id**
     - Unique task identifier in the DAG
     - ``str``
     - Yes
   * - **autocommit**
     - Automatically commit transactions
     - ``bool`` / ``True``
     - No
   * - ****kwargs**
     - Additional ``BaseOperator`` arguments
     - —
     - No

Examples
========

1. IRIS Raw SQL Demo
--------------------

.. literalinclude:: ../dags/01_IRIS_Raw_SQL_Demo.py
   :language: python
   :linenos:
   :caption: dags/01_IRIS_Raw_SQL_Demo.py

2. IRIS ORM Demo (SQLAlchemy + pandas)
--------------------------------------

Uses the only currently reliable bulk-insert method for IRIS via pandas:

.. literalinclude:: ../dags/example_sqlalchemy_dag.py
   :language: python
   :linenos:
   :caption: dags/02_IRIS_ORM_Demo.py
   :emphasize-lines: 47-48

   .. note::
      ``chunksize=1`` with ``method="multi"`` is currently the most reliable way to perform bulk inserts into InterSystems IRIS using pandas/SQLAlchemy.

3. Synthetic Sales Pipeline
---------------------------

Generate and load realistic synthetic sales data efficiently:

.. literalinclude:: ../dags/03_IRIS_Load_CSV_Synthetic_Demo.py
   :language: python
   :linenos:
   :caption: dags/03_IRIS_Load_CSV_Synthetic_Demo.py

License
=======

This project is licensed under the `Apache License 2.0 <https://opensource.org/licenses/Apache-2.0>`_.