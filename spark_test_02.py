from pyspark.sql import SparkSession


def run_spark_job():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("My Spark Application") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("info")

    # For example:
    df = spark.read.schema(
        "Index INTEGER, customer_id STRING, first_name STRING, last_name STRING, company STRING, city STRING, "
        "country STRING, phone1 STRING, phone2 STRING, email STRING, subscription STRING, website STRING").csv(
        "C:\\Users\\hp sys\\Downloads\\customers_data.csv")

    df.show(5, False)

    # Stop the SparkSession
    spark.stop()
    # spark.streams.awaitAnyTermination()


# Invoke the function at the end
if __name__ == "__main__":
    run_spark_job()
