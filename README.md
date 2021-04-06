# Custom XCom Backend Tutorial
This repo contains supporting files and an example DAG for setting up a custom XCom Backend in Airflow 2.0. A guide with detailed information on how to use this example will be published soon.

## Tutorial Content
The example use case covered in this tutorial is setting up a custom XCom Backend on AWS S3 to store XComs resulting from the [Great Expectations operator](https://registry.astronomer.io/providers/great-expectations).

## Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:

 - Install the [Astronomer CLI](https://www.astronomer.io/docs/enterprise/v0.23/develop/cli-quickstart)
 - Clone this repo somewhere locally and navigate to it in your terminal
 - Initialize an Astronomer project by running `astro dev init`
 - Start Airflow locally by running `astro dev start`
 - Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there
