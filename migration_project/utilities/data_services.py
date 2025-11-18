

import os
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import expr, lit,col
import psycopg2
import importlib
import json


# read the configured json data from the given path
def read_config(config_path):
    # Read the JSON file containing the mapping data
    json_file = os.path.abspath(config_path)
    with open(json_file) as f:
        json_data = json.load(f)
    return json_data


# apply the transformations against the dataframe
def apply_transformation(df: DataFrame, transformations):
    for transformation in transformations:
        column_name = transformation['column_name']
        expression = transformation['expression']
        df = df.withColumn(column_name, expr(expression))
    return df


# delete record from target database(POSTGRES) using the SQL driver
def delete_records_from_db(url, delete_query):
    # Spark JDBC can’t execute DELETE directly, but you can use the driver to trigger it
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    cur.execute(delete_query)
    conn.commit()
    cur.close()
    conn.close()


# read data from database and return the spark dataframe
def read_data_from_db(spark: SparkSession, url, user, pw, db_driver, query):
    df: DataFrame = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"({query}) tmp") \
        .option("user", user) \
        .option("password", pw) \
        .option("driver", db_driver) \
        .load()
    return df


# write spark dataframe into database
def write_into_db_table(df: DataFrame, target_url, target_properties, target_table, mode):
    df.write.jdbc(
        url=target_url,
        table=target_table,
        mode=mode,  # or "overwrite" if you want to replace
        properties=target_properties
    )


# generate and return the columns list which are exists in both source and target
def get_schema_aligned_columns(target_df: DataFrame, source_df: DataFrame) :
    pg_schema = target_df.schema
    cols = []
    for trg_col in pg_schema:
        trg_name = trg_col.name
        trg_name_lower = trg_name.lower()
        print(trg_name)
        src_cols = [c.lower() for c in source_df.columns]

        if trg_name_lower in src_cols:
            # cols.append(col(trg_col.name).cast(trg_col.dataType).alias(trg_col.name))
            cols.append(col(trg_col.name).alias(trg_col.name))
        else:
            cols.append(lit(None).cast(trg_col.dataType).alias(trg_col.name))

    return cols


# Generate the python objects list during the run time based on the configs
def load_services(config_path_services_to_run):
    with open(config_path_services_to_run) as f:
        cfg = json.load(f)

    service_objects = []

    for class_path in cfg["services"]:
        module_name, class_name = class_path.rsplit(".", 1)
        module_name = class_path

        # import the module dynamically
        module = importlib.import_module(module_name)

        # get class reference
        klass = getattr(module, class_name)

        # create an object
        service_objects.append(klass())

    return service_objects
