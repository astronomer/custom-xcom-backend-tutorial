"""
Example DAG that runs Great Expectations suites and stores results to the XCom backend.
"""

import os
import pandas as pd
from pendulum import datetime

from airflow import DAG
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

# This runs an expectation suite against a sample data asset. You may need to change these paths if you do not have your `data`
# directory living in a top-level `include` directory. Ensure the checkpoint yml files have the correct path to the data file.
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_file = os.path.join(
    base_path, "include", "data/yellow_tripdata_sample_2019-01.csv"
)
data_file_fail = os.path.join(
    base_path, "include", "data/yellow_tripdata_sample_2019-02.csv"
)
ge_root_dir = os.path.join(base_path, "include", "great_expectations")


with DAG(
    dag_id="example_great_expectations_dag",
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
):

    # Runs a passing suite on a dataframe
    ge_dataframe_pass = GreatExpectationsOperator(
        task_id="ge_dataframe_pass",
        expectation_suite_name="taxi.demo",
        data_context_root_dir=ge_root_dir,
        execution_engine="PandasExecutionEngine",
        data_asset_name="taxi_df_pass",
        dataframe_to_validate=pd.read_csv(
            filepath_or_buffer=data_file,
            header=0,
        ),
    )

    # Runs a failing suite on a dataframe
    ge_dataframe_fail = GreatExpectationsOperator(
        task_id="ge_dataframe_fail",
        expectation_suite_name="taxi.demo",
        data_context_root_dir=ge_root_dir,
        execution_engine="PandasExecutionEngine",
        data_asset_name="taxi_df_fail",
        dataframe_to_validate=pd.read_csv(
            filepath_or_buffer=data_file_fail,
            header=0,
            parse_dates=True,
            infer_datetime_format=True,
        ),
        fail_task_on_validation_failure=False,
    )

    # This runs a checkpoint that will pass. Make sure the checkpoint yml file has the correct path to the data file.
    ge_checkpoint_pass = GreatExpectationsOperator(
        task_id="ge_checkpoint_pass",
        run_name="ge_checkpoint_pass",
        checkpoint_name="taxi.pass.chk",
        data_context_root_dir=ge_root_dir,
    )

    ge_dataframe_pass >> ge_dataframe_fail >> ge_checkpoint_pass
