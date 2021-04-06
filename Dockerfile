FROM quay.io/astronomer/ap-airflow:2.0.0-3-buster-onbuild
ENV AIRFLOW__CORE__XCOM_BACKEND=include.s3_xcom_backend.S3XComBackend