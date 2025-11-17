import os
import json


class ShareHoldingService:

    def __init__(self):
        pass

    def get_configs(self, config_path):
        """Read JSON config and return as dictionary."""
        service_config_path = os.path.join(config_path, "shareholding_configs.json")
        with open(service_config_path, "r") as f:
            data = json.load(f)
        return data   # return dict

    def get_sql_query(self, p_mis_date):
        query = f"""
            SELECT mis_date,  
            instrument, qty, 
            avg_cost, ltp, 
            invested, 
            cur_val, 
            pnl, 
            net_chg,   
            day_chg 
            FROM    
            share_holdings
            
        """
        # where mis_date ='{p_mis_date}

        return query

