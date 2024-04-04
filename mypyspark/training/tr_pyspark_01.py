from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit
import json
from pyspark.sql.functions import count, sum, max, min

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read JSON and Union CSVs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("info")

# Read the JSON file containing the mapping data
json_file = r"D:\studymaterials\spark\data_mapping_01.json"
with open(json_file) as f:
    json_data = json.load(f)


# Define a function to read CSV files and create DataFrames
def read_csv_and_create_dataframe(path):
    df = spark.read.csv(path, header=True)  # Assuming CSV has headers
    return df


def get_all_columns(data_sources):
    all_columns = set()
    # Iterate through each data source
    for source in json_data['data_sources']:
        # Get the transformations for the current source
        transformations = source.get('transformations', [])
        # Extract column names from transformations and add them to the set
        columns = [transformation['column_name'] for transformation in transformations]
        all_columns.update(columns)
    return all_columns


# Function to align schemas, apply transformations, and normalize schema
def align_transform_normalize(df, transformations):
    # Apply transformations to the DataFrame
    for transformation in transformations:
        column_name = transformation['column_name']
        expression = transformation['expression']
        df = df.withColumn(column_name, expr(expression))
    missing_cols = all_columns - set(df.columns)
    # print(missing_cols)
    for col in missing_cols:
        df = df.withColumn(col, lit(None))
    return df


all_columns = get_all_columns(json_data['data_sources'])
print(all_columns)

# Iterate through each data source, read the CSV, and create DataFrames
dataframes = []
for source in json_data['data_sources']:
    path = source['path']
    df = read_csv_and_create_dataframe(path)
    transformations = source.get('transformations', [])
    df = align_transform_normalize(df, transformations)
    dataframes.append(df)

# Union all DataFrames
final_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), dataframes[0].schema)
for df in dataframes:
    final_df = final_df.union(df)

final_df.show(truncate=False)

final_df.groupby(["source_key", "student_id"]).agg(count("*").alias("Count"),
                                                   sum("mark").alias("Total_Score"),
                                                   max("mark").alias("Max_Score"),
                                                   min("mark").alias("Min_Score")).show(truncate=False)


# spark.streams.awaitAnyTermination()
spark.stop()
