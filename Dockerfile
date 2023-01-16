FROM quay.io/astronomer/astro-runtime:7.1.0
ENV AIRFLOW__CORE__XCOM_BACKEND=include.s3_xcom_backend_pandas.S3XComBackendPandas
