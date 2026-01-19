import json
import importlib
try:
    from awsglue.context import GlueContext
    from pyspark.sql import SparkSession, DataFrame
except ImportError:
    # Local fallback (optional but useful)
    SparkSession = None



class ShareHoldingServiceGlue:

    def __init__(self):
        pass

    def get_configs(self, spark: SparkSession, s3_folder_path: str):
        """
        Read the JSON config from S3.

        Input:
            s3_folder_path = "s3://my-bucket/configs"

        Hardcoded file:
            shareholding_configs.json

        Final Path = s3://my-bucket/configs/shareholding_configs.json
        """
        config_file_name = "shareholding_configs.json"
        json_file_path = f"{s3_folder_path}/{config_file_name}"

        # Read file as plain text
        df_text = spark.read.text(json_file_path)

        # Collect lines as a single string
        json_str = "\n".join([row.value for row in df_text.collect()])

        # Parse with Python json module
        return json.loads(json_str)


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
