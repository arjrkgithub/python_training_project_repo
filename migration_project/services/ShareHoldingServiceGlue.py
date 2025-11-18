import json
import importlib
from awsglue.context import GlueContext


class ShareHoldingServiceGlue:

    def __init__(self):
        pass

    def get_configs(self, glue_ctx: GlueContext, s3_folder_path: str):
        """
        Read the JSON config from S3.

        Input:
            s3_folder_path = "s3://my-bucket/configs"

        Hardcoded file:
            shareholding_configs.json

        Final Path = s3://my-bucket/configs/shareholding_configs.json
        """
        config_file_name ="shareholding_configs.json"
        json_file_path = f"{s3_folder_path}/{config_file_name}"

        # Read JSON from S3 into Spark DF
        df = glue_ctx.spark_session.read.json(json_file_path)

        # Expecting 1-row JSON → convert to dict
        config_dict = df.collect()[0].asDict()

        return config_dict

    def get_sql_query(self, p_mis_date: str):
        """
        Returns the SQL query string for the service.
        """
        query = f"""
            SELECT 
                mis_date,  
                instrument, 
                qty, 
                avg_cost, 
                ltp, 
                invested, 
                cur_val, 
                pnl, 
                net_chg,   
                day_chg 
            FROM share_holdings
        """
        # Optional: filter by date
        # WHERE mis_date = '{p_mis_date}'

        return query
