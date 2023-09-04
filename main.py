# main.py
import sys
import argparse
from pyspark.sql import SparkSession

from iconicretail.bronze.bronze_module import ingest_raw_data
from iconicretail.silver.silver_module import refine_enrich_data
from iconicretail.gold.gold_module import prepare_and_analyse_data

if __name__ == "__main__":
    # Call functions from each module to execute the ETL pipeline
    # Initialize a Spark session
    spark = SparkSession.builder.appName("IconicRetailStore").getOrCreate()

    parser = argparse.ArgumentParser(description="The Iconic Coding Challenge")

    parser.add_argument(
        "--input_path",
        type=str,
        required=True,
        help="Path to the input data directory."
    )

    parser.add_argument(
        "--output_path",
        type=str,
        required=True,
        help="Path to the output data directory."
    )

    args = parser.parse_args()

    input_path = args.input_path
    output_path = args.output_path

    ingest_raw_data(spark, input_path)
    refine_enrich_data(spark)
    prepare_and_analyse_data(spark, output_path)
    # deploy_and_schedule()

    # Stop the Spark session
    spark.stop()

