from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
import os

def load_bronze_data(spark, input_path):
    """
    Load data from the bronze layer (output of the bronze processing).
    """
    return spark.read.parquet(input_path)

def clean_data(df):
    """
    Clean and preprocess the data.
    """
    # Remove duplicated rows
    duplicated_rows_count = df.groupBy(df.columns).count().filter(col("count") > 1).count()

    if duplicated_rows_count > 0:
        print(f"Found {duplicated_rows_count} duplicated rows, before dropping the number of rows count={df.count()}")
        df = df.dropDuplicates()
        print(f"Dropped duplicate rows, updated count={df.count()}")

    # Look for the data anomalies
    # days_since_last_order > days_since_first_order ? It doesn't look right
    fault_count = df. \
                  select("customer_id", "days_since_first_order", "days_since_last_order"). \
                  filter("days_since_last_order > days_since_first_order"). \
                  count()
    print(f"faults={fault_count}")

    # Clearly days since last order are much higher than the first order, but theoretically
    # for order=1 both should match (y = x). Based on this, let's calculate how much these two values are apart
    days_since_order_df = df.filter("orders = 1").select("days_since_first_order", "days_since_last_order")

    # Prepare the features using VectorAssembler
    feature_columns = ["days_since_first_order"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    assembled_df = assembler.transform(days_since_order_df)
    # Create a LinearRegression model
    lr = LinearRegression(featuresCol="features", labelCol="days_since_last_order")
    # Fit the model to the data
    model = lr.fit(assembled_df)
    # Print the coefficient
    print(f"The coefficient according to Linear Regression is: {model.coefficients[0]:.2f}")

    # It could be that days_since_last_order are represented in hours ?
    # Let's correct and fix this
    corrected_days_since_last_order = round((df["days_since_last_order"] / model.coefficients[0]), -1)
    # Replace the original column with the updated values
    df = df.withColumn("days_since_last_order", corrected_days_since_last_order)

    return df


def save_silver_data(df, output_path):
    """
    Save the processed data to the silver layer.
    """
    df.write.parquet(output_path, mode="overwrite")


def register_path(directory_path, exist_ok=True):
    # Check if the directory exists
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok)
        print(f"Directory '{directory_path}' created successfully.")
    return directory_path


def refine_enrich_data(spark):
    """
    Check the data quality issues and apply various transformations to enrich the data
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Define input path for bronze data (output of the bronze processing)
    bronze_input_path = os.path.join(current_dir, "../bronze/data/out")

    # Load data from the bronze layer
    bronze_data_df = load_bronze_data(spark, bronze_input_path)

    # Clean and preprocess the data
    cleaned_data_df = clean_data(bronze_data_df)

    # Define output path for silver data
    silver_output_path = os.path.join(current_dir, "data/out")
    register_path(silver_output_path)

    # Save the processed data to the silver layer
    save_silver_data(cleaned_data_df, silver_output_path)


if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("SilverModule").getOrCreate()

    refine_enrich_data(spark)

    # Stop the Spark session
    spark.stop()
