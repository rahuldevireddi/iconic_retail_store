from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_number
import os


def load_silver_data(spark, input_path):
    """
    Load data from the silver layer (output of the silver processing).
    """
    return spark.read.parquet(input_path)


def register_path(directory_path, exist_ok=True):
    # Check if the directory exists
    if not os.path.exists(directory_path):
        os.makedirs(directory_path, exist_ok)
        print(f"Directory '{directory_path}' created successfully.")
    return directory_path


def save_gold_data(df, output_path):
    """
    Save the processed data to the gold layer.
    """
    register_path(output_path)
    df.write.csv(output_path, mode="overwrite", header=True)


def analyse_data(spark, df, output_path):
    """
    Analyse the data.
    """

    df.createTempView("customers")

    # Q1 What was the total revenue to the nearest dollar for customers who have paid by credit card?
    cc_revenue = spark.sql("""
        SELECT
            ROUND(SUM(revenue)) as creditcard_revenue
        FROM
            customers
        WHERE
            cc_payments=1
    """)
    print(f"Q1: Total revenue from customers who have paid by credit card={cc_revenue.collect()[0][0]:.2f}")
    cc_revenue = cc_revenue.withColumn("creditcard_revenue_formatted", format_number("creditcard_revenue", 2)). \
        select("creditcard_revenue_formatted")
    save_gold_data(cc_revenue, output_path + "/data/out/q1_sql_out")

    # Q2 What percentage of customers who have purchased female items have paid by credit card?
    female_transactions = spark.sql("""
        SELECT
            COUNT(*) as female_items_count_paid_by_cc
        FROM
            customers
        WHERE
            female_items>0
        GROUP BY
            cc_payments"""
                                    )
    list_female_transactions = female_transactions.collect()
    percentage_paid_by_cc = list_female_transactions[1][0] / (
                list_female_transactions[0][0] + list_female_transactions[1][0]) * 100
    print(
        f"Q2: Percentage of customers who have purchased female items have paid by credit card={percentage_paid_by_cc:.2f}%")
    output_relative_path = "/data/out/q2_sql_out"
    os.makedirs(output_path + output_relative_path, exist_ok=True)
    with open(output_path + output_relative_path + "/out.txt", "w") as f:
        f.write(
            f"Q2: Percentage of customers who have purchased female items have paid by credit card={percentage_paid_by_cc:.2f}%")

    # Q3 What was the average revenue for customers who used either iOS, Android or Desktop?
    avg_revenue = spark.sql("""
        SELECT
            ROUND(AVG(revenue),2) as AVG_REVENUE_FROM_IOS_ANDROID_PC
        FROM
            customers
        WHERE
            desktop_orders>0 OR android_orders>0 OR ios_orders>0"""
    )
    print(f"Q3: Average revenue from customers who used iOS|Android|DesktopPC is {avg_revenue.collect()[0][0]:.2f}%")
    save_gold_data(avg_revenue, output_path + "/data/out/q3_sql_out")

    # Q4 We want to run an email campaign promoting a new mens luxury brand. Can you provide a list of customers we should send to?
    # spark.sql("SELECT customer_id, items, male_items, revenue from customers").show(input.count(), truncate=False)
    result = spark.sql("""
        SELECT
            customer_id,
            ROUND(revenue/items, 2) AS avg_cost_spent_by_customer
        FROM
            customers
        WHERE
            male_items>0
        ORDER BY
            avg_cost_spent_by_customer desc
    """)
    # Get the top 10% of customer_ids
    top_10_luxury_customers = int(result.count() * (10 / 100))
    print(f"Q4: List of top {top_10_luxury_customers} customer_id's for email promotion campaign")
    # if you want full list uncomment the below line
    result = result.select("customer_id", "avg_cost_spent_by_customer"). \
        limit(top_10_luxury_customers)
    # Displays the top 20 rows by default
    result.show(truncate=False)
    save_gold_data(result, output_path + "/data/out/q4_sql_out")


def prepare_and_analyse_data(spark, output_path):
    """
    Wrapper functions to prepare the data for BI
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Define input path for silver data (output of the silver processing)
    silver_input_path = os.path.join(current_dir, "../silver/data/out")

    # Load data from the silver layer
    silver_data_df = load_silver_data(spark, silver_input_path)

    # Perform feature engineering and data enrichment
    analyse_data(spark, silver_data_df, output_path)


if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("GoldModule").getOrCreate()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = register_path(current_dir)
    prepare_and_analyse_data(spark, output_path)

    # Stop the Spark session
    spark.stop()
