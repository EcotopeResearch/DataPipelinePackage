import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import datetime, timedelta
import os

class FileProcessor:
    def __init__(self, config : ConfigManager, extension: str, start_time: datetime = None, end_time: datetime = None, raw_time_column : str = 'DateTime', 
                 time_column_format : str ='%Y/%m/%d %H:%M:%S', filename_date_format : str = "%Y%m%d%H%M%S", file_prefix : str = "", data_sub_dir : str = "",
                 date_string_start_idx : int = -17, date_string_end_idx : int = -3):
        """
        Parameters
        ----------  
        config : ecopipeline.ConfigManager
            The ConfigManager object that holds configuration data for the pipeline 
        extension : str
            File extension of raw data files as string (e.g. ".csv", ".gz", ...)
        start_time: datetime
            The point in time for which we want to start the data extraction from. This 
            is local time from the data's index.
        end_time: datetime
            The point in time for which we want to end the data extraction. This 
            is local time from the data's index.
        data_sub_dir : str
            defaults to an empty string. If the files being accessed are in a sub directory of the configured data directory, use this parameter to point there.
            e.g. if the data files you want to extract are in "path/to/data/DENT/" and your configured data directory is "path/to/data/", put "DENT/" as the data_sub_dir
        file_prefix : str
            File name prefix of raw data file if only file names with a certain prefix should be processed.
        
        Returns
        ------- 
        list[str]: 
            list of filenames 
        """
        self.extension = extension
        self.start_time = start_time
        self.end_time = end_time
        self.raw_time_column = raw_time_column
        self.time_column_format = time_column_format
        self.filename_date_format = filename_date_format
        self.date_string_start_idx = date_string_start_idx
        self.date_string_end_idx = date_string_end_idx
        self.file_prefix = file_prefix
        self.data_sub_dir = data_sub_dir
        self.raw_df = pd.DataFrame()
        try:
            filenames = self.extract_files(config)
            self.raw_df = self.raw_files_to_df(filenames)
        except Exception as e:
            print(f"File extraction failed: {e}")
            raise e
        
    def get_raw_data(self) -> pd.DataFrame:
        return self.raw_df

    def extract_files(self, config: ConfigManager) -> list[str]:
        """
        Function returns a list of all file names in the processors assigned directory with file names that fall between the start and end date (if such dates exist)
        
        Returns
        ------- 
        list[str]: 
            list of filenames 
        """
        os.chdir(os.getcwd())
        filenames = []
        full_data_path = f"{config.data_directory}{self.data_sub_dir}"
        for file in os.listdir(full_data_path):
            if file.endswith(self.extension) and file.startswith(self.file_prefix):
                full_filename = os.path.join(full_data_path, file)
                filenames.append(full_filename)

        if not self.start_time is None:
            filenames = self.extract_new(filenames)

        return filenames
    
    def extract_new(self, filenames: list[str]) -> list[str]:
        
        endTime_int = self.end_time
        startTime_int = int(self.start_time.strftime(self.filename_date_format))
        if not self.end_time is None:
            endTime_int = int(self.end_time.strftime(self.filename_date_format)
                            )
        return_list = list(filter(lambda filename: int(filename[self.date_string_start_idx:self.date_string_end_idx]) >= startTime_int and (endTime_int is None or int(filename[self.date_string_start_idx:self.date_string_end_idx]) < endTime_int), filenames))
        return return_list
    
    def _read_file_into_df(self, file_name : str) -> pd.DataFrame:
        data = pd.read_csv(file_name)
        return data

    def raw_files_to_df(self, filenames : list[str]) -> pd.DataFrame:
        temp_dfs = []
        for file in filenames:
            try:
                data = self._read_file_into_df(file)
                if len(data) != 0:
                    temp_dfs.append(data)
            except FileNotFoundError:
                print("File Not Found: ", file)
                continue
            except Exception as e:
                print(f"Error reading {file}: {e}")
                continue
                    
        if len(temp_dfs) <= 0:
            print("No data for timefarme.")
            return pd.DataFrame()
        
        df = pd.concat(temp_dfs, ignore_index=False)

        # if create_time_pt_idx:
        #     df['time_pt'] = pd.to_datetime(df[self.raw_time_column], format=self.time_column_format)
        #     df.set_index('time_pt', inplace=True)

        return df