from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import json
import uuid
import os

class S3XComBackendPandas(BaseXCom):
    PREFIX = "xcom_s3://"
    BUCKET_NAME = "s3-xcom-backend-example"

    @staticmethod
    def serialize_value(
        value,
        key=None,
        task_id=None,
        dag_id=None,
        run_id=None,
        map_index= None,
        **kwargs
    ):

        hook = S3Hook(aws_conn_id="s3_xcom_backend_conn")
        
        # added serialization method if the value passed is a Pandas dataframe
        # the contents are written to a local temporary csv file
        if isinstance(value, pd.DataFrame):
            filename = "data_" + str(uuid.uuid4()) + ".csv"
            s3_key = f"{run_id}/{task_id}/{filename}"

            value.to_csv(filename)

        # this section handles serialization of GX CheckpointResult objects
        if not isinstance(value, (str, dict, list)):
            filename = "data_" + str(uuid.uuid4()) + ".json"
            s3_key = f"{run_id}/{task_id}/{filename}"

            with open(filename, 'w') as f:
                json.dump(json.loads(str(value)), f)

        # if the value passed a str, dict or list, use standard JSON serialization
        else:
            filename = "data_" + str(uuid.uuid4()) + ".json"
            s3_key = f"{run_id}/{task_id}/{filename}"

            with open(filename, 'a+') as f:
                json.dump(value, f)

        hook.load_file(
            filename=filename,
            key=s3_key,
            bucket_name=S3XComBackendPandas.BUCKET_NAME,
            replace=True
        )

        os.remove(filename)

        reference_string = S3XComBackendPandas.PREFIX + s3_key

        return BaseXCom.serialize_value(value=reference_string)

    @staticmethod
    def deserialize_value(result):
        result = BaseXCom.deserialize_value(result)

        hook = S3Hook(aws_conn_id="s3_xcom_backend_conn")
        key = result.replace(S3XComBackendPandas.PREFIX, "")

        filename = hook.download_file(
            key=key,
            bucket_name=S3XComBackendPandas.BUCKET_NAME,
            local_path="/tmp"
        )

        # added deserialization option to convert a CSV back to a dataframe
        if key.split(".")[-1] == "csv":
            output = pd.read_csv(filename)
        # if the key does not end in 'csv' use JSON deserialization
        else:
            with open(filename, 'r') as f:
                output = json.load(f)

        os.remove(filename)

        return output