import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import datetime, timedelta
import os

class APIExtractor:
    def __init__(self, config : ConfigManager, start_time: datetime = None, end_time: datetime = None, create_csv : bool = True, csv_prefix : str = ""):
        """
        Parameters
        ----------  
        config : ecopipeline.ConfigManager
            The ConfigManager object that holds configuration data for the pipeline. The config manager
            must contain information to connect to the api, i.e. the api user name and password as well as
            the device id for the device the data is being pulled from.
        start_time: datetime
            The point in time for which we want to start the data extraction from. This 
            is local time from the data's index. 
        end_time: datetime
            The point in time for which we want to end the data extraction. This 
            is local time from the data's index. 
        create_csv : bool
            create csv files as you process such that API need not be relied upon for reprocessing
        query_hours : float
            number of hours to query at a time from ThingsBoard API

        device_id_overwrite : str
            Overwrites device ID for API pull
        csv_prefix : str
            prefix to add to the csv title
        """
        try:
            self.raw_df = self.raw_data_to_df(config, start_time, end_time)
            if create_csv and not self.raw_df.empty:
                filename = f"{csv_prefix}{start_time.strftime('%Y%m%d%H%M%S')}.csv"
                original_directory = os.getcwd()
                os.chdir(config.data_directory)
                self.raw_df.to_csv(filename, index_label='time_pt')
                os.chdir(original_directory)
                print(f"Created raw data CSV file: {filename}")
        except Exception as e:
            print(f"API data extraction failed: {e}")
            raise e
        
    def raw_data_to_df(self, config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
        return pd.DataFrame

    def get_raw_data(self) -> pd.DataFrame:
        return self.raw_df
    
    def _get_float_value(self, value):
        try:
            ret_val = float(value)
            return ret_val
        except (ValueError, TypeError):
            return None