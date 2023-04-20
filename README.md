# Parquet to BigQuery Dataflow Pipeline
This repository contains an Apache Beam pipeline that reads Parquet files from Google Cloud Storage (GCS), performs ETL transformations, and writes the processed data to a BigQuery table. The pipeline is designed to run on Google Cloud Dataflow.

## Overview
The pipeline is built using Apache Beam and includes the following main components:

1- Reading Parquet files from a GCS bucket.
2- Parsing the Parquet files and extracting the data.
3- Performing custom ETL transformations on the data.
4- Writing the processed data to a BigQuery table.
5- Monitoring for new files in the GCS bucket and processing them on arrival.

The pipeline uses custom functions located in the `pipeline_trial.custom_fns` module for ETL transformations and utility functions.

### TODO:
- Add sample data to load
- Add tutorial  Google Cloud Platform (GCP) project with billing enabled.
- A GCS bucket containing Parquet files with data from the example.
- A BigQuery dataset and table to store the processed data.

## Acknowledments
Thanks to [kevenpinto](https://github.com/kevenpinto/beam_flex_demo) which provides sample code and a [Medium](https://medium.com/cts-technologies/building-and-testing-dataflow-flex-templates-80ef6d1887c6) tutorial. 