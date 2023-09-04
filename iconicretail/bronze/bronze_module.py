from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import hashlib
import zipfile
import os


def unzip_test_data(directory_path, dest_path):
    """
    Extract the data according to the specified logic.
    """

    print(f"Raw data input path {directory_path}")

    passwd = hashlib.sha256(b'welcometotheiconic').hexdigest()
    # print("passwd={}".format(passwd))

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        # Check if the file is a zip file
        if zipfile.is_zipfile(file_path):
            # Extract the contents of the zip file to the same directory
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(dest_path, pwd=passwd.encode('utf-8'))
                print(f"Unzipped: {filename}")


def validate_data(df):
    """
    Perform basic data validation checks on the DataFrame.
    """
    # Example: Check for missing values
    missing_counts = df.select([col(c).alias(c + '_missing') for c in df.columns]).count()
    if missing_counts > 0:
        # raise ValueError(f"Missing data detected in {missing_counts} rows.")
        print(f"Missing data detected in {missing_counts} rows.")


def initial_transformations(df):
    """
    Apply initial transformations to the DataFrame.
    """
    # Let's reorder the columns for easier analysis
    desired_order = [
        "customer_id", "days_since_first_order", "days_since_last_order", "is_newsletter_subscriber",
        "orders", "items", "cancels", "returns", "different_addresses", "shipping_addresses", "devices",
        "vouchers", "cc_payments", "paypal_payments", "afterpay_payments", "apple_payments", "female_items",
        "male_items", "unisex_items", "wapp_items", "wftw_items", "mapp_items", "wacc_items", "macc_items",
        "mftw_items", "wspt_items", "mspt_items", "curvy_items", "sacc_items", "msite_orders", "desktop_orders",
        "android_orders", "ios_orders", "other_device_orders", "work_orders", "home_orders", "parcelpoint_orders",
        "other_collection_orders", "average_discount_onoffer", "average_discount_used", "revenue"
    ]
    df = df.select(*desired_order)

    # Let's drop these columns, we will adjust this list later based on feature use
    not_needed_columns = [
        "is_newsletter_subscriber",
        "cancels", "returns", "different_addresses", "shipping_addresses", "devices",
        "vouchers", "paypal_payments", "afterpay_payments", "apple_payments",
        "unisex_items", "wapp_items", "wftw_items", "mapp_items", "wacc_items", "macc_items",
        "mftw_items", "wspt_items", "mspt_items", "curvy_items", "sacc_items", "msite_orders",
        "other_device_orders", "work_orders", "home_orders", "parcelpoint_orders",
        "other_collection_orders", "average_discount_onoffer", "average_discount_used",
    ]
    df = df.drop(*not_needed_columns)
    return df


def register_path(directory_path, exist_ok=True):
    # Check if the directory exists
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok)
        print(f"Directory '{directory_path}' created successfully.")
    return directory_path

def ingest_raw_data(spark, input_path=""):
    """
    Ingest the cleaned data for further layers
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))

    raw_input_path = ""
    if input_path == "":
        raw_input_path = register_path(current_dir + "/data/raw")
    else:
        raw_input_path = input_path

    print(raw_input_path)

    # Extract the data
    output_path = current_dir + "/data/in"
    unzip_test_data(raw_input_path, output_path)

    # Ingest raw data (output from unzipped data content)
    raw_data_df = spark.read.option("inferSchema", "True").json(output_path)

    # Perform data validation
    validate_data(raw_data_df)

    # Apply initial transformations
    bronze_data_df = initial_transformations(raw_data_df)

    # Save the bronze data to a location for the next processing stage
    bronze_output_path = register_path(current_dir + "/data/out")
    bronze_data_df.write.parquet(bronze_output_path, mode="overwrite")


if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("BronzeModule").getOrCreate()

    ingest_raw_data(spark)

    # Stop the Spark session
    spark.stop()
