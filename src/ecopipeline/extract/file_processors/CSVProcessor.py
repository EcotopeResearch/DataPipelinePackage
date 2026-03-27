import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import datetime, timedelta
from ecopipeline.extract.FileProcessor import FileProcessor

class CSVProcessor(FileProcessor):
    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None, raw_time_column: str = 'DateTime', 
                 time_column_format: str = '%Y/%m/%d %H:%M:%S', filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "", 
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3, csv_sub_type : str = None):
        self.csv_sub_type = csv_sub_type

        super().__init__(config, ".csv", start_time, end_time, raw_time_column, time_column_format, filename_date_format, 
                         file_prefix, data_sub_dir, date_string_start_idx, date_string_end_idx)
        
    def _read_file_into_df(self, file_name : str) -> pd.DataFrame:
        data = super()._read_file_into_df(file_name)
        if len(data) != 0:
            if self.csv_sub_type == "modbus":
                if self.raw_time_column in data.columns:
                    #prepend modbus prefix
                    prefix = file_name.split("/")[-1].split('.')[0]
                    data["time_pt"] = pd.to_datetime(data[self.raw_time_column])
                    data = data.set_index("time_pt")
                    data = data.rename(columns={col: f"{prefix}_{col}".replace(" ","_") for col in data.columns})
                else:
                    print(f"Error reading {file_name}: No '{self.raw_time_column}' column found.")
            elif self.csv_sub_type == "dent":
                if len(data.columns) >= 3:
                    # in dent file format, the first column can be removed and the second and third columns are date and time respectively
                    data.columns = ['temp', 'date', 'time'] + data.columns.tolist()[3:]
                    data = data.drop(columns=['temp'])
                    data['time_pt'] = pd.to_datetime(data['date'] + ' ' + data['time'])
                    data = data.set_index("time_pt")
                else:
                    print(f"Error reading {file_name}: No time columns found.")
            elif self.csv_sub_type == "flow":
                if all(x in data.columns.to_list() for x in ['Month','Day','Year','Hour','Minute','Second']):
                    # Convert the datetime string to datetime
                    date_str = data['Year'].astype(str) + '-' + data['Month'].astype(str).str.zfill(2) + '-' + data['Day'].astype(str).str.zfill(2) + ' ' + data['Hour'].astype(str).str.zfill(2) + ':' + data['Minute'].astype(str).str.zfill(2) + ':' + data['Second'].astype(str).str.zfill(2)
                    data['time_pt'] = pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
                    data = data.set_index("time_pt")
                else:
                    print(f"Error reading {file_name}: No time columns found.")
            else:
                data['time_pt'] = pd.to_datetime(data[self.raw_time_column], format=self.time_column_format)
                data.set_index('time_pt', inplace=True)
                
        return data