from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import json
import uuid

class S3XComBackend(BaseXCom):
    PREFIX = "xcom_s3://"
    BUCKET_NAME = "kenten-xcom-backend-testing"

    @staticmethod
    def serialize_value(value: Any):
        if not isinstance(value, (str, dict, list)):
            hook        = S3Hook()
            key         = "data_" + str(uuid.uuid4())
            filename    = f"{key}.json"

            with open(filename, 'w') as f:
                json.dump(json.loads(str(value)), f)

            hook.load_file(
                filename=filename,
                key=key,
                bucket_name=S3XComBackend.BUCKET_NAME,
                replace=True
            )
            value = S3XComBackend.PREFIX + key
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(S3XComBackend.PREFIX):
            hook    = S3Hook()
            key     = result.replace(S3XComBackend.PREFIX, "")
            filename = hook.download_file(
                key=key,
                bucket_name=S3XComBackend.BUCKET_NAME,
                local_path="/tmp"
            )
            result = json.load(filename)
        return result