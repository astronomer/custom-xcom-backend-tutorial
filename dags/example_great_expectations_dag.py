from airflow import DAG
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

import logging
import os
from datetime import datetime, timedelta

# This runs an expectation suite against a sample data asset. You may need to change these paths if you do not have your `data`
# directory living in a top-level `include` directory. Ensure the checkpoint yml files have the correct path to the data file.
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_file = os.path.join(base_path, 'include',
                         'data/yellow_tripdata_sample_2019-01.csv')
ge_root_dir = os.path.join(base_path, 'include', 'great_expectations')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='example_great_expectations_dag',
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False 
    ) as dag:

    ge_batch_kwargs_pass = GreatExpectationsOperator(
        task_id='ge_batch_kwargs_pass',
        expectation_suite_name='taxi.demo',
        batch_kwargs={
            'path': data_file,
            'datasource': 'data__dir'
        },
        data_context_root_dir=ge_root_dir,
    )

    # This runs an expectation suite against a data asset that passes the tests
    ge_batch_kwargs_list_pass = GreatExpectationsOperator(
        task_id='ge_batch_kwargs_list_pass',
        assets_to_validate=[
            {
                'batch_kwargs': {
                    'path': data_file,
                    'datasource': 'data__dir'
                },
                'expectation_suite_name': 'taxi.demo'
            }
        ],
        data_context_root_dir=ge_root_dir,
    )

    # This runs a checkpoint that will pass. Make sure the checkpoint yml file has the correct path to the data file.
    ge_checkpoint_pass = GreatExpectationsOperator(
        task_id='ge_checkpoint_pass',
        run_name='ge_airflow_run',
        checkpoint_name='taxi.pass.chk',
        data_context_root_dir=ge_root_dir,
    )


    
    ge_batch_kwargs_list_pass >> ge_batch_kwargs_pass >> ge_checkpoint_pass
