# Custom XCom Backend Tutorial

This repo contains the setup for a custom XCom Backend tutorial as shown in the ["Set up a custom XCom backend using cloud-based or local object storage" tutorial](https://docs.astronomer.io/learn/xcom-backend-tutorial). 
Additionally the custom XCom Backends in this repository can handle CheckpointResult objects from GX.

## How to use this repository

1. Make sure you have the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) installed on your computer. It is the easiest way to run Airflow locally in docker. 
2. Clone this repository locally. 
3. Inside the cloned repository run `astro dev init` (respond yes to the prompt informing you that the directory is not empty, the command will add a couple of local files to the repository).
4. By default the repo is configured to use a custom XCom backend in S3. If you want to use GCS, Azure or MinIO replace the second line in the dockerfile with the path to the custom XCom backend class you'd like to use. The classes are located in the include folder.
5. Start Airflow locally by running `astro dev start`.
6. In your browser go to `localhost:8080`, log in with `admin:admin` to see the Airflow UI.
7. Define an Airflow connection for your chosen backend provider as explained [in Step 3 of the tutorial](https://docs.astronomer.io/learn/xcom-backend-tutorial#step-3-create-a-connection). Make sure to chose the connection ids listed in the tutorial.
8. Run the DAGs.


## DAGs

This tutorial repository contains 2 DAGs:

- `fetch_pokemon_data_dag`: Fetches data about your favorite Pokemon from an API and calculates a new metric. This DAG pushes a Pandas dataframe to a custom XCom backend that serializes it as a CSV file. This is not possible without a custom XCom backend. More information on this DAG can be found in [Step 7 of the tutorial](https://docs.astronomer.io/learn/xcom-backend-tutorial#step-7-run-a-dag-passing-pandas-dataframes-via-xcom).
- `example_great_expectations_dag`: Runs a sample [GreatExpectations](https://greatexpectations.io/) suite on a CSV file. This DAG pushes a CheckpointResult object to XCom which gets serialized by a custom method. This also is not possible without a custom XCom backend. 
