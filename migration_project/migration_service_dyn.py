from pyspark.sql import SparkSession, DataFrame
from utilities.data_services import (
    read_config,
    read_data_from_db,
    write_into_db_table,
    apply_transformation,
    get_schema_aligned_columns,
    delete_records_from_db,
    load_services
)
from datetime import datetime, timedelta


def migration_process():
    # has to be extracted from  parameter feed
    # p_mis_date = '17-NOV-2025'
    p_mis_date = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y").upper()
    p_config_path = "C:/Users/hp sys/PycharmProjects/pythonProject/migration_project/configs"

    # to store the results dataframe
    results = []

    # read the configs that which will tell the services to be executed
    config_path_services_to_run = f"{p_config_path}/services_to_run.json"

    # Get the python executable objects for the services
    services = load_services(config_path_services_to_run)

    # Create a Spark Session
    spark = SparkSession.builder \
        .appName("MIG_ORA_TO_POSTGRES_PROCESS") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    # set the Log mode
    spark.sparkContext.setLogLevel("info")

    # read the source and target database configs
    src_target_config = f"{p_config_path}/src_target_configs.json"
    db_configs = read_config(src_target_config)

    # fetch and assign the source DB configs
    src_url = db_configs["source_db_configs"]["url"]
    src_user = db_configs["source_db_configs"]["user"]
    src_pw = db_configs["source_db_configs"]["password"]
    src_driver = db_configs["source_db_configs"]["driver"]

    # fetch and assign target configs
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
    target_url_for_sql = f"{target_sql_driver}://{target_user}:{target_pw}@{target_host}:{target_port}/{target_database}"

    # fetching records and store it into dataframe
    for service in services:
        src_qry = service.get_sql_query(p_mis_date)
        qry_configs = service.get_configs(p_config_path)

        # read query configs
        target_table = qry_configs["targetTable"]
        transformations = qry_configs["transformations"]
        load_type = qry_configs["load_type"]
        incremental_column = qry_configs["incremental_column"]

        # read source data from database
        df_source: DataFrame = read_data_from_db(spark, src_url, src_user, src_pw, src_driver, src_qry)

        # apply the transformations against the source data
        df_transformed: DataFrame = apply_transformation(df_source, transformations)

        # read target schema
        target_schema_qry = f"select * from {target_table}  where 1 = 2"
        df_target_empty: DataFrame = read_data_from_db(spark, target_url, target_user, target_pw,
                                                       target_driver, target_schema_qry)
        # get Cols List expression to select the data to sync with target
        cols = get_schema_aligned_columns(df_transformed, df_target_empty)

        # get the dataframe which is syn with target table
        df_aligned = df_transformed.select(cols)

        # store the results dataframe into List along with write parameters
        results.append({
            "df_aligned": df_aligned,
            "load_type": load_type,
            "target_table": target_table,
            "incremental_column": incremental_column
            })

    # iterate the dataframe output lists and write it into Database
    for result in results:
        load_type = result["load_type"]
        target_table = result["target_table"]
        incremental_column = result["incremental_column"]
        df_aligned = result["df_aligned"]
        if load_type == "incremental":
            v_del_sql = f"delete from  {target_table}  where {incremental_column } = '{p_mis_date}'"
            print(v_del_sql)
            delete_records_from_db(target_url_for_sql, v_del_sql)
            print("deletion done successfully")
            mode = "append"
        else:
            mode = "overwrite"
            print("going to be overwritten")

        # call the write function to write the dataframe into target database
        write_into_db_table(df_aligned, target_url, target_properties, target_table, mode)
        print("Written process completed")


# ENTRY POINT
if __name__ == "__main__":
    print("Process started")
    migration_process()














