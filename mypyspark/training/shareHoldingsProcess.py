
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import expr, lit
import json
from pyspark.sql.functions import count, sum, max, min,avg
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType


def read_config(config_path):
    # Read the JSON file containing the mapping data
    json_file = os.path.abspath(config_path)
    with open(json_file) as f:
        json_data = json.load(f)
    return json_data


def read_csv(spark, file_path, read_schema):
    df = spark.read.option("header", True).schema(read_schema).csv(file_path)
    return df


def get_data_type(type_str):
    return {
        "string": StringType(),
        "int": IntegerType(),
        "double": DoubleType(),
        "date": DateType()
    }.get(type_str.lower(), StringType())  # default to StringType


def build_schema(schema_config):
    return StructType([
        StructField(field["col_name"], get_data_type(field["col_type"]), True)
        for field in schema_config
    ])


def apply_transformation(spark, df, transformations):
    for transformation in transformations:
        column_name = transformation['column_name']
        expression = transformation['expression']
        df = df.withColumn(column_name, expr(expression))
    return df


def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Read JSON and Union CSVs") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("info")

    configs = read_config(r"mapping_data\shareholdings_mapping.json")
    print(configs["shareHoldings"]["sourceCSVPath"])

    csv_path = configs["shareHoldings"]["sourceCSVPath"]
    read_schema = build_schema(configs["shareHoldings"]["readSchema"])

    df: DataFrame = read_csv(spark, csv_path, read_schema)

    transformations = configs["shareHoldings"]["transformations"]

    df2: DataFrame = apply_transformation(spark,df,transformations)

    df3 = df2.groupBy("instrument").agg(sum("quantity").alias("quantity_sum"),
        avg("avg_cost").alias("avg_cost"),
        sum("ltp").alias("ltp"))

    df3.show(20,False)


if __name__ == "__main__":
    print("Hell0")
    main()














