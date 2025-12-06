from typing import Any, Dict


def get_provider_info() -> Dict[str, Any]:
    return {
        "package-name": "airflow-provider-iris",
        "name": "IRIS Airflow Provider",
        "description": "Intersystems IRIS provider for Apache Airflow.",
        "hook-class-names": ["airflow_provider_iris.hooks.iris_hook.IrisHook"],
        "extra-links": [],
        "versions": ["0.1.0"],
    }
