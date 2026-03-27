import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import gzip
import json
import mysql.connector.errors as mysqlerrors
from datetime import datetime, timedelta
from ecopipeline.extract.FileProcessor import FileProcessor

class JSONProcessor(FileProcessor):
    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None, raw_time_column: str = 'time', 
                 time_column_format: str = '%Y/%m/%d %H:%M:%S', filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "", 
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3, zip_files : bool = True, time_zone : str = 'US/Pacific'):
        self.zip_files = zip_files
        self.time_zone = time_zone
        if self.zip_files:
            ext = ".gz"
        else:
            ext = ".json"

        super().__init__(config, ext, start_time, end_time, raw_time_column, time_column_format, filename_date_format, 
                         file_prefix, data_sub_dir, date_string_start_idx, date_string_end_idx)
        
    def raw_files_to_df(self, filenames : list[str], create_time_pt_idx : bool = True) -> pd.DataFrame:
        # no need to process time columns
        return super().raw_files_to_df(filenames, False)

    def _read_file_into_df(self, file_name : str) -> pd.DataFrame:
        # TODO: how will this change if zip_files : bool = False
        try:
            data = gzip.open(file_name)
        except FileNotFoundError as e:
            print("File Not Found: ", file_name)
            return pd.DataFrame()
        try:
            data = json.load(data)
        except json.decoder.JSONDecodeError:
            print('Empty or invalid JSON File')
            return pd.DataFrame()
        
        norm_data = pd.json_normalize(data, record_path=['sensors'], meta=['device', 'connection', 'time'])
        if len(norm_data) != 0:

            norm_data["time_pt"] = pd.to_datetime(norm_data[self.raw_time_column])

            norm_data["time_pt"] = norm_data["time_pt"].dt.tz_localize("UTC").dt.tz_convert(self.time_zone)
            norm_data = pd.pivot_table(norm_data, index="time_pt", columns="id", values="data")
            # Iterate over the index and round up if necessary (work around for json format from sensors)
            for i in range(len(norm_data.index)):
                if norm_data.index[i].minute == 59 and norm_data.index[i].second == 59:
                    norm_data.index.values[i] = norm_data.index[i] + pd.Timedelta(seconds=1)
        return norm_data