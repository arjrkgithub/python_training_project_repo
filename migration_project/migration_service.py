
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import expr, lit,col
import json
from pyspark.sql.functions import count, sum, max, min,avg
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import psycopg2
from psycopg2 import sql


def read_config(config_path):
    # Read the JSON file containing the mapping data
    json_file = os.path.abspath(config_path)
    with open(json_file) as f:
        json_data = json.load(f)
    return json_data


def apply_transformation(spark, df, transformations):
    for transformation in transformations:
        column_name = transformation['column_name']
        expression = transformation['expression']
        df = df.withColumn(column_name, expr(expression))
    return df


def delete_records_from_db(url,delete_query):
    # Spark JDBC can’t execute DELETE directly, but you can use the driver to trigger it
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    cur.execute(delete_query)
    conn.commit()
    cur.close()
    conn.close()


def read_data_from_db(spark, url, user, pw, db_driver, query):
    df = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"({query}) tmp") \
        .option("user", user) \
        .option("password", pw) \
        .option("driver", db_driver) \
        .load()
    return df


def write_into_db_table(spark,df, target_url,target_properties,target_table,mode):
    df.write.jdbc(
        url=target_url,
        table=target_table,
        mode=mode,  # or "overwrite" if you want to replace
        properties=target_properties
    )


def get_schema_aligned_columns(targetDF : DataFrame, sourceDF : DataFrame) :
    pg_schema = targetDF.schema
    cols = []
    for trg_col in pg_schema:
        trg_name = trg_col.name
        trg_name_lower = trg_name.lower()
        print(trg_name)
        src_cols = [c.lower() for c in sourceDF.columns]

        if trg_name_lower in src_cols:
            # cols.append(col(trg_col.name).cast(trg_col.dataType).alias(trg_col.name))
            cols.append(col(trg_col.name).alias(trg_col.name))
        else:
            cols.append(lit(None).cast(trg_col.dataType).alias(trg_col.name))

    return cols


def migration_process():
    # has to be extracted from  parameter feed
    p_mis_date = '16-NOV-2025'

    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Read JSON and Union CSVs") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("info")

    db_configs = read_config(r"configs/src_target_configs.json")

    qry_configs = read_config(r"configs/shareholding_configs.json")

    # read source DB
    src_url = db_configs["source_db_configs"]["url"]
    src_user = db_configs["source_db_configs"]["user"]
    src_pw = db_configs["source_db_configs"]["password"]
    src_driver = db_configs["source_db_configs"]["driver"]

    # read target configs
    target_url = db_configs["target_db_configs"]["url"]
    target_user = db_configs["target_db_configs"]["user"]
    target_pw = db_configs["target_db_configs"]["password"]
    target_driver = db_configs["target_db_configs"]["driver"]
    target_sql_driver = db_configs["target_db_configs"]["sqldriver"]
    target_host = db_configs["target_db_configs"]["host"]
    target_port = db_configs["target_db_configs"]["port"]
    target_database = db_configs["target_db_configs"]["database"]
    target_properties = {
        "user": target_user,
        "password": target_pw,
        "driver": target_driver
    }
    target_url_for_sql= f"{target_sql_driver}://{target_user}:{target_pw}@{target_host}:{target_port}/{target_database}"

    # read query configs
    src_qry = qry_configs["sourceQuery"]
    target_table = qry_configs["targetTable"]
    transformations = qry_configs["transformations"]
    load_type = qry_configs["load_type"]
    incremental_column = qry_configs["incremental_column"]

    df_source : DataFrame = read_data_from_db(spark, src_url, src_user, src_pw, src_driver, src_qry)

    df_transformed : DataFrame = apply_transformation(spark,df_source,transformations)

    # df_transformed.show(100, False)

    target_schema_qry = f"select * from {target_table}  where 1 = 2"

    df_target_empty: DataFrame = read_data_from_db(spark, target_url, target_user, target_pw, target_driver, target_schema_qry)

    cols = get_schema_aligned_columns(df_transformed,df_target_empty)

    df_aligned = df_transformed.select(cols)

    if load_type == "incremental":
        v_del_sql = f"delete from  {target_table}  where {incremental_column } = '{p_mis_date}'"
        print(v_del_sql)
        delete_records_from_db(target_url_for_sql,v_del_sql)
        print("deletion done successfully")
        mode ="append"
    else:
        mode ="overwrite"
        print("going to be overwritten")

    write_into_db_table(spark, df_aligned, target_url, target_properties, target_table, mode)

    print("writtern process completed")
    # df_source.show(5,False)


if __name__ == "__main__":
    print("Process started")
    migration_process()














