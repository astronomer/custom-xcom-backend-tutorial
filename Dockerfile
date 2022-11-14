FROM quay.io/astronomer/astro-runtime:6.0.3
ENV AIRFLOW__CORE__XCOM_BACKEND=include.s3_xcom_backend.S3XComBackend
