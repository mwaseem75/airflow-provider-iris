from setuptools import setup
from pathlib import Path
__version__ = "0.2.8"


this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

"""Perform the package airflow-provider-iris setup."""
setup(
    name="airflow-provider-iris",
    version=__version__,
    description="airflow provider for intersystems",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=airflow_provider_iris.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=[
        "airflow_provider_iris",
        "airflow_provider_iris.hooks",
        "airflow_provider_iris.operators",       
    ],
    install_requires=["apache-airflow>=2.0", "sqlalchemy-iris", "pandas"],
    setup_requires=["setuptools", "wheel"],
    author="Muhammad Waseem",
    author_email="muhammadwas@outlook.com",
    url="https://github.com/mwaseem75",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.7",
)
