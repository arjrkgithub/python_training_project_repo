import sys
import json
from datetime import datetime, timedelta
from pyspark.context import SparkContext

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

from utilities.data_services_glue import (
    read_config,
    read_data_from_db,
    read_data_from_db_jdbc,
    write_into_db_table,
    write_into_db_table_local,
    apply_transformation,
    get_schema_aligned_columns,
    delete_records_from_db,
    delete_records_from_db_local,
    load_services
)


# ---------------------------------------------------------------
# Main Process
# ---------------------------------------------------------------
def migration_process(glue_ctx: GlueContext, args):
    sc = glue_ctx.spark_session
    v_run_mode = "local"
    v_read_type ="jdbc" # jdbc / glue

    # Compute MIS Date
    p_mis_date = args["MIS_DATE"]

    # S3 config path passed as Glue Job parameter
    p_config_path = args["CONFIG_PATH"]         # Example: s3://bucket/configs/

    # Container to store results
    results = []

    # Read list of services to run
    config_path_services_to_run = f"{p_config_path}/services_to_run_glue.json"
    services = load_services(sc, config_path_services_to_run)

    # Read source/target database configs
    src_target_config = f"{p_config_path}/src_target_configs.json"
    db_configs = read_config(sc, src_target_config)

    # Source DB connection
    src_connection_name = db_configs["source_db_configs"]["connection_name"]
    if v_run_mode == "local":
        source_url = db_configs["source_db_configs"]["url"]
        source_driver = db_configs["source_db_configs"]["driver"]
        source_user = db_configs["source_db_configs"]["user"]
        source_pw = db_configs["source_db_configs"]["password"]
    else:
        s_jdbc_conf = glue_ctx.extract_jdbc_conf(src_connection_name)
        source_url = s_jdbc_conf["url"]
        source_driver = s_jdbc_conf["driver"]
        source_user = s_jdbc_conf["user"]
        source_pw = s_jdbc_conf["password"]


    # Target DB connection
    tgt_connection_name = db_configs["target_db_configs"]["connection_name"]
    if v_run_mode == "local":
        target_url = db_configs["target_db_configs"]["url"]
        target_driver = db_configs["target_db_configs"]["driver"]
        target_user = db_configs["target_db_configs"]["user"]
        target_pw = db_configs["target_db_configs"]["password"]
        target_port = db_configs["target_db_configs"]["port"]
        target_db = db_configs["target_db_configs"]["database"]
        target_host = db_configs["target_db_configs"]["host"]
    else:
        t_jdbc_conf = glue_ctx.extract_jdbc_conf(tgt_connection_name)
        target_url = t_jdbc_conf["url"]
        target_driver = t_jdbc_conf["driver"]
        target_user = t_jdbc_conf["user"]
        target_pw = t_jdbc_conf["password"]
        target_port = t_jdbc_conf["port"]
        target_db = t_jdbc_conf["database"]
        target_host = t_jdbc_conf["host"]

    target_properties = {
        "user": target_user,
        "password": target_pw,
        "driver": target_driver
    }
    target_url_for_sql = f"{target_driver}://{target_user}:{target_pw}@{target_host}:{target_port}/{target_db}"

    # -----------------------------------------------------------
    # MAIN EXECUTION LOOP
    # -----------------------------------------------------------
    for service in services:

        # Get SQL query for this service
        src_qry = service.get_sql_query(p_mis_date)

        # Read JSON configs for target table & transformations
        qry_configs = service.get_configs(sc, p_config_path)
        target_table = qry_configs["targetTable"]
        transformations = qry_configs["transformations"]
        load_type = qry_configs["load_type"]
        incremental_column = qry_configs["incremental_column"]

        # -------------------------------------------------------
        # READ SOURCE DATA (Glue Connection)
        # -------------------------------------------------------
        if v_read_type == "glue":
            df_source = read_data_from_db(
                 glue_ctx=glue_ctx,
                 connection_name=src_connection_name,
                 query=src_qry
            )
        else:
            df_source = read_data_from_db_jdbc(sc, source_url, source_user, source_pw, source_driver, src_qry)

        # Apply transformations
        df_transformed = apply_transformation(df_source, transformations)

        print("######################################source extraction done")

        # -------------------------------------------------------
        # READ TARGET TABLE SCHEMA
        # -------------------------------------------------------
        target_schema_qry = f"select * from {target_table} where 1 = 2"

        if v_read_type == "glue":
            df_target_empty = read_data_from_db(
               glue_ctx=glue_ctx,
               connection_name=tgt_connection_name,
               query=target_schema_qry
            )
        else:
            df_target_empty = read_data_from_db_jdbc(sc, target_url, target_user, target_pw,
                                                      target_driver, target_schema_qry)

        # df_target_empty = DynamicFrame.fromDF(df_target_empty_local, glue_ctx, "df_name")
        print("######################################target extraction done")
        # Schema Alignment
        cols = get_schema_aligned_columns(df_target_empty, df_transformed)
        df_aligned = df_transformed.select(cols)

        print("######################################Aligned DF extracted")

        # Store result for writing later
        results.append({
            "df_aligned": df_aligned,
            "load_type": load_type,
            "target_table": target_table,
            "incremental_column": incremental_column
        })

    # -----------------------------------------------------------
    # WRITE LOOP
    # -----------------------------------------------------------
    for result in results:
        load_type = result["load_type"]
        target_table = result["target_table"]
        incremental_column = result["incremental_column"]
        df_aligned = result["df_aligned"]

        if load_type == "incremental":

            delete_sql = f"""
                DELETE FROM {target_table}
                WHERE {incremental_column} = '{p_mis_date}'
            """

            print(delete_sql)

            if v_run_mode == "aws":
                # DELETE using Glue Connection
                delete_records_from_db(
                    glue_ctx=glue_ctx,
                    connection_name=tgt_connection_name,
                    delete_query=delete_sql
                )
            else:
                # delete_sql, host, port, dbname, user, password
                delete_records_from_db_local(delete_sql, target_host, target_port, target_db, target_user, target_pw)

            print("Deletion completed")
            write_mode = "append"

        else:
            print("Performing full table overwrite")
            write_mode = "overwrite"

        # Write using Glue JDBC output
        if v_run_mode == "aws":
            write_into_db_table(
                glue_ctx=glue_ctx,
                df=df_aligned,
                connection_name=tgt_connection_name,
                target_table=target_table,
                mode=write_mode
            )
        else:
            write_into_db_table_local(df_aligned, target_url, target_properties, target_table, write_mode)


        print("Write completed")


# ---------------------------------------------------------------
# ENTRY POINT — Glue Job
# ---------------------------------------------------------------
if __name__ == "__main__":
    print("#####################################WS Glue Migration Process Started")

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONFIG_PATH","MIS_DATE"])

    glue_ctx = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    migration_process(glue_ctx, args)

    job.commit()
    print("#################################AWS Glue Migration Completed")
