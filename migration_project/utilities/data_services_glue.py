import os
import sys
import json
import importlib
import psycopg2
try:
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
except ImportError:
    # Local fallback (optional but useful)
    GlueContext = None
    getResolvedOptions = None
    Job = None
    DynamicFrame = None


from pyspark.sql.functions import expr, lit, col
from pyspark.context import SparkContext
from pyspark.sql import SparkSession,DataFrame


# -------------------------------------------------------------------
# Read JSON config file (from S3)
# -------------------------------------------------------------------
def read_config_old(spark: SparkSession, config_path):
    """
    Reads JSON from S3 or local path.
    Example S3 Path: s3://bucket/configs/mapping.json
    """
    df = spark.read \
        .option("multiLine", "true") \
        .option("mode", "PERMISSIVE") \
        .json(config_path)

    return df.collect()[0].asDict()


def read_config(spark: SparkSession, config_path):
    # Read file as plain text
    df_text = spark.read.text(config_path)

    # Collect lines as a single string
    json_str = "\n".join([row.value for row in df_text.collect()])

    # Parse with Python json module
    return json.loads(json_str)

# -------------------------------------------------------------------
# Apply transformations to dataframe
# -------------------------------------------------------------------
def apply_transformation(df: DataFrame, transformations):
    for transformation in transformations:
        column_name = transformation["column_name"]
        expression = transformation["expression"]
        df = df.withColumn(column_name, expr(expression))
    return df


# -------------------------------------------------------------------
# DELETE using Glue JDBC (instead of psycopg2)
# Glue 4.0 supports executeStatement()
# -------------------------------------------------------------------
def delete_records_from_db(glue_ctx: GlueContext, connection_name, delete_query):
    """
    Executes DELETE statement using Glue connection
    Requires Glue 4.0 or 5.0
    """
    glue_ctx.execute_statement(
        connection_name=connection_name,
        statement=delete_query
    )


def delete_records_from_db_local(delete_sql, host, port, dbname, user, password):
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(delete_sql)
    cur.close()
    conn.close()


# -------------------------------------------------------------------
# Read from database using Glue connection
# -------------------------------------------------------------------
def read_data_from_db(glue_ctx: GlueContext, connection_name, query):
    """
    Read data from ANY JDBC database (Oracle, Postgres, MySQL)
    using AWS Glue Connection.
    Returns Spark DataFrame.
    """
    dynamic_frame = glue_ctx.create_dynamic_frame.from_options(
        connection_type="jdbc",
        connection_options={
            "connectionName": connection_name,
            "query": query
        }
    )

    return dynamic_frame.toDF()


def read_data_from_db_jdbc(
        spark: SparkSession,
        url: str,
        user: str,
        pw: str,
        driver: str,
        query: str,
        partition_column: str = None,
        lower_bound: int = None,
        upper_bound: int = None,
        num_partitions: int = None,
        fetchsize: int = 10000
) -> DataFrame:
    """
    Reads data from a JDBC source into a Spark DataFrame.

    Args:
        spark: SparkSession object
        url: JDBC URL
        user: DB username
        pw: DB password
        driver: JDBC driver class
        query: SQL query (use 'query' instead of 'dbtable' for safety)
        partition_column: Column name for parallel reads (numeric only)
        lower_bound: Lower bound of partition column
        upper_bound: Upper bound of partition column
        num_partitions: Number of partitions for parallel reads
        fetchsize: Number of rows per fetch

    Returns:
        Spark DataFrame
    """

    reader = spark.read.format("jdbc") \
        .option("url", url) \
        .option("query", query) \
        .option("user", user) \
        .option("password", pw) \
        .option("driver", driver) \
        .option("fetchsize", fetchsize)

    # Apply partitioning only if all required params are provided
    if partition_column and lower_bound is not None and upper_bound is not None and num_partitions:
        reader = reader.option("partitionColumn", partition_column) \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .option("numPartitions", num_partitions)

    df = reader.load()
    return df


# -------------------------------------------------------------------
# Write DataFrame to database using Glue connection
# -------------------------------------------------------------------

def write_into_db_table(glue_ctx: GlueContext, df: DataFrame, connection_name, target_table, mode="append"):

    # Convert DataFrame to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glue_ctx, "df")

    glue_ctx.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="jdbc",        # use jdbc, NOT "postgresql"
        connection_options={
            "connectionName": connection_name,
            "dbtable": target_table,
            "preactions": "",
            "postactions": "",
            "mode": mode
        }
    )


# write spark dataframe into database
def write_into_db_table_local(df: DataFrame, target_url, target_properties, target_table, mode):
    df.write.jdbc(
        url=target_url,
        table=target_table,
        mode=mode,  # or "overwrite" if you want to replace
        properties=target_properties
    )


# -------------------------------------------------------------------
# Align schemas between source and target
# -------------------------------------------------------------------
def get_schema_aligned_columns(target_df: DataFrame, source_df: DataFrame):
    pg_schema = target_df.schema
    cols = []

    src_cols = {c.lower() for c in source_df.columns}

    for trg_col in pg_schema:
        trg_name = trg_col.name
        trg_lower = trg_name.lower()

        if trg_lower in src_cols:
            cols.append(col(trg_name).alias(trg_name))
        else:
            cols.append(lit(None).cast(trg_col.dataType).alias(trg_name))

    return cols


# -------------------------------------------------------------------
# Loads service classes dynamically
# -------------------------------------------------------------------

def load_services(spark: SparkSession, config_s3_path: str):
    """
    Reads services JSON from S3 using GlueContext and dynamically loads classes.

    Example JSON:
    {
        "services": [
            "services.ShareHoldingService"
        ]
    }

    Example S3 path: s3://my-bucket/configs/services_to_run.json
    """
    # Use GlueContext to read JSON from S3
    # df = glue_ctx.spark_session.read.json(config_s3_path)
    # cfg = df.collect()[0].asDict()  # Convert single-row DataFrame to dict

    cfg = read_config(spark, config_s3_path)


    service_objects = []
    for class_path in cfg.get("services", []):
        module_name, class_name = class_path.rsplit(".", 1)
        module_name = class_path
        module = importlib.import_module(module_name)
        klass = getattr(module, class_name)
        service_objects.append(klass())

    return service_objects


