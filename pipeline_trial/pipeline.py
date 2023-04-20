import os
import time
from google.cloud import storage
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromParquet
from apache_beam.io import WriteToBigQuery
from apache_beam.io.gcp import bigquery
from apache_beam.runners import DataflowRunner
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.options.pipeline_options import GoogleCloudOptions
import pandas as pd
import ast
import numpy as np
import argparse
import logging

## Local imports
from pipeline_trial.custom_fns import etl
from pipeline_trial.custom_fns import utils


# Define the pipeline
def run(file_name, args, beam_args):
    beam_options = PipelineOptions(beam_args)

    full_file_path = 'gs://' + args.bucket + args.parquetpath + file_name

    # BigQuery table information
    output_table = f"{args.project}:{args.dataset}.{args.table}"

  # Your existing pipeline code ...
    with beam.Pipeline(runner=DataflowRunner(), options=beam_options) as p:
        records_and_schema = (
            p | "Read from Parquet" >> ReadFromParquet(full_file_path)
            | "Parse Parquet" >> beam.Map(utils.parse_parquet)
            | "ETL Attributes" >> beam.FlatMap(etl.clean_attributes)
        )

        records = records_and_schema | "Extract Records" >> beam.Map(lambda x: x[0])
        schema = records_and_schema | "Extract Schema" >> beam.Map(lambda x: x[1]) | "Get Schema" >> beam.combiners.Sample.FixedSizeGlobally(1)
        schema = beam.coders.registry.get_coder(schema)

        records | "Write to BigQuery" >> WriteToBigQuery(
            output_table,
            schema=schema,
            write_disposition=bigquery.BigQueryDisposition.WRITE_APPEND,
            create_disposition=bigquery.BigQueryDisposition.CREATE_IF_NEEDED,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project',
        required=True,
        help='The project id where magic happens.')
    parser.add_argument(
        '--bucket',
        required=True,
        help='The bucket name where the files are stored.')
    parser.add_argument(
        '--parquetpath',
        required=True,
        help='The file path for the new parquet files.')
    parser.add_argument(
        '--dataset',
        required=True,
        help='The BigQuery dataset where data is gonna be stored.')
    parser.add_argument(
        '--table',
        required=True,
        help='The BigQuery table to store the data.')
    parser.add_argument(
        '--runtime',
        required=False,
        help='The time in minutes to re-run the pipeline.', 
        type=int,
        default=1)

    args, beam_args = parser.parse_known_args()

    processed_files_path = "processed_files.txt" ## TODO - make this a runtime parameter not a local file
    
    interval = 60 * args.runtime  # 1 minute default

    while True:
        new_files = utils.get_new_files(args.project, args.bucket, args.parquetpath, processed_files_path)

        for file_name in new_files:
            print(f"Processing file: {file_name}")
            run(file_name, args, beam_args)
            utils.update_processed_files(file_name, processed_files_path)

        print(f"Sleeping for {interval} seconds")
        time.sleep(interval)
