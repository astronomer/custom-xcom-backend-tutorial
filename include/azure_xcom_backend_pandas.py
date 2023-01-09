from airflow.models.xcom import BaseXCom
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import pandas as pd
import json
import uuid
import os

class WasbXComBackendPandas(BaseXCom):
    PREFIX = "xcom_wasb://"
    CONTAINER_NAME = "custom-xcom-backend"

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

        hook = WasbHook(wasb_conn_id="azure_xcom_backend_conn")
        
        # added serialization method if the value passed is a Pandas dataframe
        # the contents are written to a local temporary csv file
        if isinstance(value, pd.DataFrame):
            filename = "data_" + str(uuid.uuid4()) + ".csv"
            blob_key = f"{run_id}/{task_id}/{filename}"

            value.to_csv(filename)

        # if the value passed is not a Pandas dataframe, attempt to use
        # JSON serialization
        else:
            filename = "data_" + str(uuid.uuid4()) + ".json"
            blob_key = f"{run_id}/{task_id}/{filename}"

            with open(filename, 'a+') as f:
                json.dump(value, f)

        hook.load_file(
            file_path=filename,
            container_name=WasbXComBackendPandas.CONTAINER_NAME,
            blob_name=blob_key
        )

        os.remove(filename)

        reference_string = WasbXComBackendPandas.PREFIX + blob_key

        return BaseXCom.serialize_value(value=reference_string)

    @staticmethod
    def deserialize_value(result):
        result = BaseXCom.deserialize_value(result)

        hook = WasbHook(wasb_conn_id="azure_xcom_backend_conn")
        blob_key = result.replace(WasbXComBackendPandas.PREFIX, "")

        hook.get_file(
            blob_name=blob_key,
            container_name=WasbXComBackendPandas.CONTAINER_NAME,
            file_path="my_xcom_file",
            offset=0,
            length=100000
        )

        # added deserialization option to convert a CSV back to a dataframe
        if blob_key.split(".")[-1] == "csv":
            output = pd.read_csv("my_xcom_file")
        # if the key does not end in 'csv' use JSON deserialization
        else:
            with open("my_xcom_file", 'r') as f:
                output = json.load(f)

        os.remove("my_xcom_file")

        return output