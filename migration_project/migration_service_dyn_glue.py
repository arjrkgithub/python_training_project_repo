import sys
import json
from datetime import datetime, timedelta

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext

from utilities.data_services_glue import (
    read_config,
    read_data_from_db,
    write_into_db_table,
    apply_transformation,
    get_schema_aligned_columns,
    delete_records_from_db,
    load_services
)


# ---------------------------------------------------------------
# Main Process
# ---------------------------------------------------------------
def migration_process(glue_ctx, args):
    sc = glue_ctx.spark_session

    # Compute MIS Date
    #p_mis_date = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y").upper()
    p_mis_date = args["mis_date"]

    # S3 config path passed as Glue Job parameter
    p_config_path = args["CONFIG_PATH"]         # Example: s3://bucket/configs/

    # Container to store results
    results = []

    # Read list of services to run
    config_path_services_to_run = f"{p_config_path}/services_to_run.json"
    services = load_services(glue_ctx, config_path_services_to_run)

    # Read source/target database configs
    src_target_config = f"{p_config_path}/src_target_configs.json"
    db_configs = read_config(src_target_config)

    # Source DB connection
    src_connection_name = db_configs["source_db_configs"]["connection_name"]

    # Target DB connection
    tgt_connection_name = db_configs["target_db_configs"]["connection_name"]

    # -----------------------------------------------------------
    # MAIN EXECUTION LOOP
    # -----------------------------------------------------------
    for service in services:

        # Get SQL query for this service
        src_qry = service.get_sql_query(p_mis_date)

        # Read JSON configs for target table & transformations
        qry_configs = service.get_configs(p_config_path)
        target_table = qry_configs["targetTable"]
        transformations = qry_configs["transformations"]
        load_type = qry_configs["load_type"]
        incremental_column = qry_configs["incremental_column"]

        # -------------------------------------------------------
        # READ SOURCE DATA (Glue Connection)
        # -------------------------------------------------------
        df_source = read_data_from_db(
            glue_ctx=glue_ctx,
            connection_name=src_connection_name,
            query=src_qry
        )

        # Apply transformations
        df_transformed = apply_transformation(df_source, transformations)

        # -------------------------------------------------------
        # READ TARGET TABLE SCHEMA
        # -------------------------------------------------------
        target_schema_qry = f"select * from {target_table} where 1 = 2"

        df_target_empty = read_data_from_db(
            glue_ctx=glue_ctx,
            connection_name=tgt_connection_name,
            query=target_schema_qry
        )

        # Schema Alignment
        cols = get_schema_aligned_columns(df_target_empty, df_transformed)
        df_aligned = df_transformed.select(cols)

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

            # DELETE using Glue Connection
            delete_records_from_db(
                glue_ctx=glue_ctx,
                connection_name=tgt_connection_name,
                delete_query=delete_sql
            )

            print("Deletion completed")
            write_mode = "append"

        else:
            print("Performing full table overwrite")
            write_mode = "overwrite"

        # Write using Glue JDBC output
        write_into_db_table(
            glue_ctx=glue_ctx,
            df=df_aligned,
            connection_name=tgt_connection_name,
            target_table=target_table,
            mode=write_mode
        )

        print("Write completed")


# ---------------------------------------------------------------
# ENTRY POINT — Glue Job
# ---------------------------------------------------------------
if __name__ == "__main__":
    print("AWS Glue Migration Process Started")

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "CONFIG_PATH","MIS_DATE"])

    glue_ctx = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    migration_process(glue_ctx, args)

    job.commit()
    print("AWS Glue Migration Completed")
