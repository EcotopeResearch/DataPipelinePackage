from typing import List
import pandas as pd
import re
from ftplib import FTP
from datetime import datetime, timedelta
import gzip
import os
import json
from ecopipeline.utils.unit_convert import temp_c_to_f, divide_num_by_ten, windspeed_mps_to_knots, precip_cm_to_mm, conditions_index_to_desc
from ecopipeline import ConfigManager
import numpy as np
import sys
from pytz import timezone, utc
import mysql.connector.errors as mysqlerrors
import requests
import subprocess


def get_last_full_day_from_db(config : ConfigManager) -> datetime:
    """
    Function retrieves the last line from the database with the most recent datetime 
    in local time.
    
    Parameters
    ---------- 
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline
    
    Returns
    ------- 
    datetime:
        end of last full day populated in database or default past time if no data found
    """
    # config_dict = get_login_info(["minute"], config)
    table_config_dict = config.get_db_table_info(["minute"])
    # db_connection, db_cursor = connect_db(config_info=config_dict['database'])
    db_connection, db_cursor = config.connect_db()
    return_time = datetime(year=2000, month=1, day=9, hour=23, minute=59, second=0).astimezone(timezone('US/Pacific')) # arbitrary default time
    
    try:
        db_cursor.execute(
            f"select * from {table_config_dict['minute']['table_name']} order by time_pt DESC LIMIT 1")

        last_row_data = pd.DataFrame(db_cursor.fetchall())
        if len(last_row_data.index) > 0:
            last_time = last_row_data[0][0] # get time from last_data_row[0][0] TODO probably better way to do this
            
            if ((last_time.hour != 23) or (last_time.minute != 59)):
                return_time = last_time - timedelta(days=1)
                return_time = return_time.replace(hour=23, minute=59, second=0)
            else:
                return_time = last_time 
        else:
            print("Database has no previous data. Using default time to extract data.")
    except mysqlerrors.Error:
        print("Unable to find last timestamp in database. Using default time to extract data.")

    db_cursor.close()
    db_connection.close()
    
    return return_time

def get_db_row_from_time(time: datetime, config : ConfigManager) -> pd.DataFrame:
    """
    Extracts a row from the applicable minute table in the database for the given datetime or returns empty dataframe if none exists

    Parameters
    ---------- 
    time : datetime
        The time index to get the row from
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline 
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing the row or empty if no row exists for the timestamp
    """
    # config_dict = get_login_info(["minute"], config_file_path)
    # db_connection, db_cursor = connect_db(config_info=config_dict['database'])
    table_config_dict = config.get_db_table_info(["minute"])
    db_connection, db_cursor = config.connect_db()
    row_data = pd.DataFrame()

    try:
        db_cursor.execute(
            f"SELECT * FROM {table_config_dict['minute']['table_name']} WHERE time_pt = '{time}'")
        row = db_cursor.fetchone()
        if row is not None:
            col_names = [desc[0] for desc in db_cursor.description]
            row_data = pd.DataFrame([row], columns=col_names)
    except mysqlerrors.Error as e:
        print("Error executing sql query.")
        print("MySQL error: {}".format(e))

    db_cursor.close()
    db_connection.close()

    return row_data

def extract_new(startTime: datetime, filenames: List[str], decihex = False, timeZone: str = None, endTime: datetime = None, dateStringStartIdx : int = -17) -> List[str]:
    """
    Function filters the filenames to only those equal to or newer than the date specified startTime.
    If filenames are in deciheximal, The function can still handel it. Note that for some projects,
    files are dropped at irregular intervals so data cannot be filtered by exact date.

    Currently, this function expects file names to be in one of two formats:

    1. normal (set decihex = False) format assumes file names are in format such that characters [-17,-3] in the file names string
        are the files date in the form "%Y%m%d%H%M%S"
    2. deciheximal (set decihex = True) format assumes file names are in format such there is a deciheximal value between a '.' and '_' character in each filename string
        that has a deciheximal value equal to the number of seconds since January 1, 1970 to represent the timestamp of the data in the file.

    Parameters
    ----------  
    startTime: datetime
        The point in time for which we want to start the data extraction from. This 
        is local time from the data's index. 
    filenames: List[str]
        List of filenames to be filtered by those equal to or newer than startTime
    decihex: bool
        Defaults to False. Set to True if filenames contain date of data in deciheximal format
    timeZone: str
        The timezone for the indexes in the output dataframe as a string. Must be a string recognized as a 
        time stamp by the pandas tz_localize() function https://pandas.pydata.org/docs/reference/api/pandas.Series.tz_localize.html
        defaults to None
    dateStringStartIdx: int
        The character index in each file where the date in format "%Y%m%d%H%M%S" starts. Default is -17 (meaning 17 characters from the end of the filename string)
    
    Returns
    -------
    List[str]: 
        Filtered list of filenames
    """
    
    if decihex: 
        base_date = datetime(1970, 1, 1)
        file_dates = [pd.Timestamp(base_date + timedelta(seconds = int(re.search(r'\.(.*?)_', filename).group(1), 16))) for filename in filenames] #convert decihex to dates, these are in utc
        if timeZone == None:
            file_dates_local = [file_date.tz_localize('UTC').tz_localize(None) for file_date in file_dates] #convert utc 
        else:
            file_dates_local = [file_date.tz_localize('UTC').tz_convert(timezone(timeZone)).tz_localize(None) for file_date in file_dates] #convert utc to local zone with no awareness

        return_list = [filename for filename, local_time in zip(filenames, file_dates_local) if local_time > startTime and (endTime is None or local_time < endTime)]


    else: 
        startTime_int = int(startTime.strftime("%Y%m%d%H%M%S"))
        return_list = list(filter(lambda filename: int(filename[dateStringStartIdx:dateStringStartIdx+14]) >= startTime_int and (endTime is None or int(filename[dateStringStartIdx:dateStringStartIdx+14]) < int(endTime.strftime("%Y%m%d%H%M%S"))), filenames))
    return return_list

def extract_files(extension: str, config: ConfigManager, data_sub_dir : str = "", file_prefix : str = "") -> List[str]:
    """
    Function takes in a file extension and subdirectory and returns a list of paths files in the directory of that type.

    Parameters
    ----------  
    extension : str
        File extension of raw data files as string (e.g. ".csv", ".gz", ...)
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline 
    data_sub_dir : str
        defaults to an empty string. If the files being accessed are in a sub directory of the configured data directory, use this parameter to point there.
        e.g. if the data files you want to extract are in "path/to/data/DENT/" and your configured data directory is "path/to/data/", put "DENT/" as the data_sub_dir
    file_prefix : str
        File name prefix of raw data file if only file names with a certain prefix should be processed.
    
    Returns
    ------- 
    List[str]: 
        List of filenames 
    """
    os.chdir(os.getcwd())
    filenames = []
    full_data_path = f"{config.data_directory}{data_sub_dir}"
    for file in os.listdir(full_data_path):
        if file.endswith(extension) and file.startswith(file_prefix):
            full_filename = os.path.join(full_data_path, file)
            filenames.append(full_filename)

    return filenames


def json_to_df(json_filenames: List[str], time_zone: str = 'US/Pacific') -> pd.DataFrame:
    """
    Function takes a list of gz/json filenames and reads all files into a singular dataframe.

    Parameters
    ----------  
    json_filenames: List[str]
        List of filenames to be processed into a single dataframe 
    time_zone: str
        The timezone for the indexes in the output dataframe as a string. Must be a string recognized as a 
        time stamp by the pandas tz_localize() function https://pandas.pydata.org/docs/reference/api/pandas.Series.tz_localize.html
        defaults to 'US/Pacific'
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing data from all files with column headers the same as the variable names in the files
    """
    temp_dfs = []
    for file in json_filenames:
        try:
            data = gzip.open(file)
        except FileNotFoundError:
            print("File Not Found: ", file)
            return
        try:
            data = json.load(data)
        except json.decoder.JSONDecodeError:
            print('Empty or invalid JSON File')
            return

        norm_data = pd.json_normalize(data, record_path=['sensors'], meta=['device', 'connection', 'time'])
        if len(norm_data) != 0:

            norm_data["time"] = pd.to_datetime(norm_data["time"])

            norm_data["time"] = norm_data["time"].dt.tz_localize("UTC").dt.tz_convert(time_zone)
            norm_data = pd.pivot_table(norm_data, index="time", columns="id", values="data")
            # Iterate over the index and round up if necessary (work around for json format from sensors)
            for i in range(len(norm_data.index)):
                if norm_data.index[i].minute == 59 and norm_data.index[i].second == 59:
                    norm_data.index.values[i] = norm_data.index[i] + pd.Timedelta(seconds=1)
            temp_dfs.append(norm_data)

    df = pd.concat(temp_dfs, ignore_index=False)
    return df  

def csv_to_df(csv_filenames: List[str], mb_prefix : bool = False, round_time_index : bool = True, create_time_pt_idx : bool = False, original_time_columns : str = 'DateTime', time_format : str ='%Y/%m/%d %H:%M:%S') -> pd.DataFrame:
    """
    Function takes a list of csv filenames and reads all files into a singular dataframe. Use this for aquisuite data. 

    Parameters
    ----------  
    csv_filenames: List[str]
        List of filenames to be processed into a single dataframe 
    mb_prefix: bool
        A boolean that signifys if the data is in modbus form- if set to true, will prepend modbus prefix to each raw varriable name
    round_time_index: bool
        A boolean that signifys if the dataframe timestamp indexes should be rounded down to the nearest minute.
        Should be set to False if there is no column in the data frame called 'time(UTC)' to index on.
        Defaults to True.
    create_time_pt_idx: bool
        set to true if there is a time column in the csv that you wish to convert to a 'time_pt' index. False otherwise
        defaults to false.
    original_time_columns : str
        The name of the time column in the raw datafiles. defaults to 'DateTime'. Only used if create_time_pt_idx is True
        
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing data from all files with column headers the same as the variable names in the files 
        (with prepended modbus prefix if mb_prefix = True)
    """
    temp_dfs = []
    for file in csv_filenames:
        try:
            data = pd.read_csv(file)
        except FileNotFoundError:
            print("File Not Found: ", file)
            return
        except Exception as e:
            print(f"Error reading {file}: {e}")
            #raise e  # Raise the caught exception again
            continue
        
        if len(data) != 0:
            if mb_prefix:
                if "time(UTC)" in data.columns:
                    #prepend modbus prefix
                    prefix = file.split('.')[0].split("/")[-1]
                    data["time(UTC)"] = pd.to_datetime(data["time(UTC)"])
                    data = data.set_index("time(UTC)")
                    data = data.rename(columns={col: f"{prefix}_{col}".replace(" ","_") for col in data.columns})
                else:
                    print(f"Error reading {file}: No 'time(UTC)' column found.")
                    continue
                
            temp_dfs.append(data)
    df = pd.concat(temp_dfs, ignore_index=False)

    if create_time_pt_idx:
        df['time_pt'] = pd.to_datetime(df[original_time_columns], format=time_format)
        df.set_index('time_pt', inplace=True)

    if round_time_index:
        #round down all seconds, 99% of points come in between 0 and 30 seconds but there are a few that are higher
        df.index = df.index.floor('T')
        
        #group and sort index
        df = df.groupby(df.index).mean(numeric_only=True)
        df.sort_index(inplace = True)

    return df

def dent_csv_to_df(csv_filenames: List[str], round_time_index : bool = True) -> pd.DataFrame:
    """
    Function takes a list of csv filenames and reads all files into a singular dataframe. Use this for aquisuite data. 

    Parameters
    ----------  
    csv_filenames: List[str]
        List of filenames to be processed into a single dataframe 
    round_time_index: bool
        A boolean that signifys if the dataframe timestamp indexes should be rounded down to the nearest minute.
        Should be set to False if there is no column in the data frame called 'time(UTC)' to index on.
        Defaults to True.
        
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing data from all files with column headers the same as the variable names in the files 
        (with prepended modbus prefix if mb_prefix = True)
    """
    temp_dfs = []
    for file in csv_filenames:
        try:
            # data headers are on row 13
            data = pd.read_csv(file, skiprows=12)
        except FileNotFoundError:
            print("File Not Found: ", file)
            return
        except Exception as e:
            print(f"Error reading {file}: {e}")
            #raise e  # Raise the caught exception again
            continue

        if len(data) != 0:
            if len(data.columns) >= 3:
                # in dent file format, the first column can be removed and the second and third columns are date and time respectively
                data.columns = ['temp', 'date', 'time'] + data.columns.tolist()[3:]
                data = data.drop(columns=['temp'])
                data['time_pt'] = pd.to_datetime(data['date'] + ' ' + data['time'])
                data = data.set_index("time_pt")
            else:
                print(f"Error reading {file}: No time columns found.")
                continue
                
            temp_dfs.append(data)
    df = pd.concat(temp_dfs, ignore_index=False)
    
    if round_time_index:
        #round down all seconds, 99% of points come in between 0 and 30 seconds but there are a few that are higher
        df.index = df.index.floor('T')
        
        #group and sort index
        df = df.groupby(df.index).mean(numeric_only=True)
        df.sort_index(inplace = True)

    return df

def flow_csv_to_df(csv_filenames: List[str], round_time_index : bool = True) -> pd.DataFrame:
    """
    Function takes a list of csv filenames and reads all files into a singular dataframe. Use this for aquisuite data. 

    Parameters
    ----------  
    csv_filenames: List[str]
        List of filenames to be processed into a single dataframe 
    round_time_index: bool
        A boolean that signifys if the dataframe timestamp indexes should be rounded down to the nearest minute.
        Should be set to False if there is no column in the data frame called 'time(UTC)' to index on.
        Defaults to True.
        
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing data from all files with column headers the same as the variable names in the files 
        (with prepended modbus prefix if mb_prefix = True)
    """
    temp_dfs = []
    for file in csv_filenames:
        try:
            # data headers are on row 6
            data = pd.read_csv(file, skiprows=6)
        except FileNotFoundError:
            print("File Not Found: ", file)
            return
        except Exception as e:
            print(f"Error reading {file}: {e}")
            #raise e  # Raise the caught exception again
            continue

        if len(data) != 0:
            if all(x in data.columns.to_list() for x in ['Month','Day','Year','Hour','Minute','Second']):
                # Convert the datetime string to datetime
                date_str = data['Year'].astype(str) + '-' + data['Month'].astype(str).str.zfill(2) + '-' + data['Day'].astype(str).str.zfill(2) + ' ' + data['Hour'].astype(str).str.zfill(2) + ':' + data['Minute'].astype(str).str.zfill(2) + ':' + data['Second'].astype(str).str.zfill(2)
                data['time_pt'] = pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
                data = data.set_index("time_pt")
            else:
                print(f"Error reading {file}: No time columns found.")
                continue
                
            temp_dfs.append(data)
    df = pd.concat(temp_dfs, ignore_index=False)
    
    if round_time_index:
        #round down all seconds, 99% of points come in between 0 and 30 seconds but there are a few that are higher
        df.index = df.index.floor('T')
        
        #group and sort index
        df = df.groupby(df.index).mean(numeric_only=True)
        df.sort_index(inplace = True)

    return df

def msa_to_df(csv_filenames: List[str], mb_prefix : bool = False, time_zone: str = 'US/Pacific') -> pd.DataFrame:
     """
    Function takes a list of csv filenames and reads all files into a singular dataframe. Use this for MSA data. 

    Parameters
    ----------  
    csv_filenames : List[str]
        List of filenames 
    mb_prefix : bool
        signifys in modbus form- if set to true, will append modbus prefix to each raw varriable
    timezone : str
        local timezone, default is pacific
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing data from all files
    """
     temp_dfs = []
     for file in csv_filenames:
        try:
            data = pd.read_csv(file)
        except FileNotFoundError:
            print("File Not Found: ", file)
            return
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
        
        if len(data) != 0:
            if mb_prefix:
                #prepend modbus prefix
                prefix = file.split('.')[0].split("/")[-1]

                data['time_pt'] = pd.to_datetime(data['DateEpoch(secs)'], unit='s',  utc=True)
                data['time_pt'] = data['time_pt'].dt.tz_convert('US/Pacific').dt.tz_localize(None)
                data.set_index('time_pt', inplace = True)
                data.drop(columns = 'DateEpoch(secs)', inplace = True)
                data = data.rename(columns={col: f"{prefix}{col}".replace(" ","_").replace("*", "_") for col in data.columns})
                
            temp_dfs.append(data)

     df = pd.concat(temp_dfs, ignore_index=False)
     
     #note sure if we should be rounding down but best I can do atm
     df.index = df.index.floor('T')

     #group and sort index
     df = df.groupby(df.index).mean()
     
     df.sort_index(inplace = True)

     return df

def small_planet_control_to_df(config: ConfigManager, csv_filenames: List[str], site: str = "", system: str = "") -> pd.DataFrame:
    """
    Function takes a list of csv filenames and reads all files into a singular dataframe. Use this for small planet control data.
    This data will have variable names equal variable_name column is Variable_Names.csv so you will not need to use the rename_sensors function
    afterwards.

    Parameters
    ---------- 
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Varriable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv")
        The csv this points to should have at least 2 columns called "variable_alias" (the raw name to be changed from) and "variable_name"
        (the name to be changed to). All columns without a cooresponding variable_name will be dropped from the dataframe.
    csv_filenames : List[str]
        List of filenames 
    site: str
        If the pipeline is processing data for a particular site with a dataframe that contains data from multiple sites that 
        need to be prossessed seperatly, fill in this optional varriable to drop data from all other sites in the returned dataframe. 
        Appropriate varriables in your Variable_Names.csv must have a matching substring to this varriable in a column called "site".
    system: str
        If the pipeline is processing data for a particular system with a dataframe that contains data from multiple systems that 
        need to be prossessed seperatly, fill in this optional varriable to drop data from all other systems in the returned dataframe. 
        Appropriate varriables in your Variable_Names.csv must have a matching string to this varriable in a column called "system"
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing data from all files
    """
    variable_names_path = config.get_var_names_path()
    try:
        variable_data = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        raise Exception("Variable names file Not Found: "+ variable_names_path)
    
    if (site != ""):
        variable_data = variable_data.loc[variable_data['site'] == site]
    if (system != ""):
        variable_data = variable_data.loc[variable_data['system'].str.contains(system, na=False)]

    variable_data = variable_data.loc[:, ['variable_alias', 'variable_name']]
    variable_data.dropna(axis=0, inplace=True)
    variable_alias = list(variable_data["variable_alias"])
    variable_true = list(variable_data["variable_name"])
    variable_alias_true_dict = dict(zip(variable_alias, variable_true))

    temp_dfs = []
    for file in csv_filenames:
        # each file contains a single variable in this format
        try:
            data = pd.read_csv(file)
        except FileNotFoundError:
            print("File Not Found: ", file)
            return
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
        
        if len(data) != 0:
            #prepend modbus prefix MOD_RTU_Nesbit_BTU_17_.Building_DHW_Energy_Total.1713078000
            prefix = file.split('.')[0].split("/")[-1]

            data['time_pt'] = pd.to_datetime(data['DateEpoch(secs)'], unit='s',  utc=True)
            data['time_pt'] = data['time_pt'].dt.tz_convert('US/Pacific').dt.tz_localize(None)
            data.set_index('time_pt', inplace = True)
            data.drop(columns = 'DateEpoch(secs)', inplace = True)
            data = data.rename(columns={col: f"{prefix}{col}".replace(" ","_").replace("*", "_") for col in data.columns})
            data.rename(columns=variable_alias_true_dict, inplace=True)
            # Because there was a name change of varriables in the middle of the data, we rename our sensors here before joining them
            # =======================================================================================================================
            # drop columns that do not have a corresponding true name
            data.drop(columns=[col for col in data if col in variable_alias], inplace=True)
            # drop columns that are not documented in variable names csv file at all
            data.drop(columns=[col for col in data if col not in variable_true], inplace=True)
            #drop null columns
            data = data.dropna(how='all')
            # =======================================================================================================================
            temp_dfs.append(data)

    df = pd.concat(temp_dfs, ignore_index=False)
    
    #note sure if we should be rounding down but best I can do atm
    df.index = df.index.floor('T')

    #group and sort index
    df = df.groupby(df.index).mean()
    
    df.sort_index(inplace = True)

    return df

def egauge_csv_to_df(csv_filenames: List[str]) -> pd.DataFrame:
    """
    Function takes a list of csv filenames and reads all files into a singular dataframe. Use this for small planet control data.
    This data will have variable names equal variable_name column is Variable_Names.csv so you will not need to use the rename_sensors function
    afterwards.

    Parameters
    ---------- 
    csv_filenames : List[str]
        List of filenames
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing data from all files
    """

    temp_dfs = []
    for file in csv_filenames:
        # each file contains a single variable in this format
        try:
            data = pd.read_csv(file)
        except FileNotFoundError:
            print("File Not Found: ", file)
            return
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue
        
        if len(data) != 0:
            #prepend modbus prefix MOD_RTU_Nesbit_BTU_17_.Building_DHW_Energy_Total.1713078000
            prefix = file.split('.')[0].split("/")[-1]

            data['time_pt'] = pd.to_datetime(data['Date & Time'], unit='s',  utc=True)
            data['time_pt'] = data['time_pt'].dt.tz_convert('US/Pacific').dt.tz_localize(None)
            data.set_index('time_pt', inplace = True)
            data.drop(columns = 'Date & Time', inplace = True)
            data = data.rename(columns={col: f"{prefix}_{col}".replace(" ","_").replace("*", "_") for col in data.columns})
            #drop null columns
            data = data.dropna(how='all')
            # =======================================================================================================================
            temp_dfs.append(data)

    df = pd.concat(temp_dfs, ignore_index=False)
    
    #note sure if we should be rounding down but best I can do atm
    df.index = df.index.floor('T')

    #group and sort index
    df = df.groupby(df.index).mean()
    
    df.sort_index(inplace = True)
    df_diff = df - df.shift(1)
    df_diff[df.shift(1).isna()] = np.nan
    df_diff.iloc[0] = np.nan

    return df_diff

def fm_api_to_df(config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
    """
    Function connects to the field manager api to pull data and returns a dataframe.

    Parameters
    ----------  
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. The config manager
        must contain information to connect to the api, i.e. the api user name and password as well as
        the device id for the device the data is being pulled from.
    startTime: datetime
        The point in time for which we want to start the data extraction from. This 
        is local time from the data's index. 
    endTime: datetime
        The point in time for which we want to end the data extraction. This 
        is local time from the data's index. 
    
    Returns
    ------- 
    pd.DataFrame: 
        Pandas Dataframe containing data from the API pull with column headers the same as the variable names in the data from the pull
    """
    api_token = config.get_fm_token()
    device_id = config.get_fm_device_id()
    url = f"https://www.fieldpop.io/rest/method/fieldpop-api/deviceDataLog?happn_token={api_token}&deviceID={device_id}"
    if not startTime is None:
        url = f"{url}&startUTCsec={startTime.timestamp()}"
    else:
        startTime = datetime(2000, 1, 1, 0, 0, 0)  # Jan 1, 2000
    if not endTime is None:
        url = f"{url}&endUTCsec={endTime.timestamp()}"
    else:
        endTime = datetime.now()

    try:
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.DataFrame()
            response = response.json()['data']
            for key, value in response.items():
                for sensor, data in value.items():
                    sensor_object = []
                    # sensor_string = f'{key}_{sensor}'
                    sensor_string = f'{sensor}'
                    for entry in data:
                        sensor_object.append({
                            'time_pt' : entry['time'],
                            sensor_string : entry['value']
                        })
                    df = pd.concat([df, pd.DataFrame(sensor_object)])

            # Convert 'time_pt' to datetime and set as index
            if not df.empty:
                df['time_pt'] = pd.to_datetime(df['time_pt'], unit='s')
                df.set_index('time_pt', inplace=True)
                df = df.sort_index()
                df = df.groupby(df.index).mean()
            return df
        elif response.status_code == 500:
            json_message = response.json()
            string_to_match = 'The log size is too large - please try again with a smaller date range.'
            if 'error' in json_message and 'message' in json_message['error'] and json_message['error']['message'] == string_to_match:
                # Calculate the midpoint between the two datetimes
                time_diff = endTime - startTime
                midpointTime = startTime + time_diff / 2
                # recursively construct the df
                df_1 = fm_api_to_df(config, startTime, midpointTime)
                df_2 = fm_api_to_df(config, midpointTime, endTime)
                df = pd.concat([df_1, df_2])
                df = df.sort_index()
                df = df.groupby(df.index).mean()
                return df
            
        print(f"Failed to make GET request. Status code: {response.status_code} {response.json()}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def pull_egauge_data(config: ConfigManager, eGauge_ids: list, eGauge_usr : str, eGauge_pw : str, num_days : int = 1):
    original_directory = os.getcwd()

    os.chdir(config.data_directory)
    try:
        for eGauge_id in eGauge_ids:
            filename = f"{eGauge_id}.{datetime.today().date().strftime('%Y%m%d%H%M%S')}.csv"
            cmd = f"wget --no-check-certificate -O {filename} \"https://egauge{eGauge_id}.egaug.es/cgi-bin/egauge-show?c&m&s=0&n=1499\" --user={eGauge_usr} --password={eGauge_pw}"

            # Running the shell command
            subprocess.run(cmd, shell=True)
    except Exception as e:
        print(f"Could not download new data from eGauge device: {e}")

    os.chdir(original_directory)
    
def get_sub_dirs(dir: str) -> List[str]:
    """
    Function takes in a directory and returns a list of the paths to all immediate subfolders in that directory. 
    This is used when multiple sites are being ran in same pipeline. 

    Parameters
    ---------- 
    dir : str
        Directory as a string.

    Returns
    ------- 
    List[str]: 
        List of paths to subfolders.
    """
    directories = []
    try:
        for name in os.listdir(dir):
            path = os.path.join(dir, name)
            if os.path.isdir(path):
                directories.append(path + "/")
    except FileNotFoundError:
        print("Folder not Found: ", dir)
        return
    return directories


def get_noaa_data(station_names: List[str], config : ConfigManager, station_ids : dict = {}) -> dict:
    """
    Function will take in a list of station names and will return a dictionary where the key is the station name and the value is a dataframe with the parsed weather data.

    Parameters
    ---------- 
    station_names : List[str]
        List of Station Names
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline
    
    Returns
    -------
    dict: 
        Dictionary with key as Station Name and Value as DF of Parsed Weather Data
    """
    formatted_dfs = {}
    weather_directory = config.get_weather_dir_path()
    try:
        noaa_dictionary = _get_noaa_dictionary(weather_directory)
        if len(station_ids.keys()) == 0:
            station_ids = {noaa_dictionary[station_name]
                : station_name for station_name in station_names if station_name in noaa_dictionary}
        noaa_filenames = _download_noaa_data(station_ids, weather_directory)
        noaa_dfs = _convert_to_df(station_ids, noaa_filenames, weather_directory)
        formatted_dfs = _format_df(station_ids, noaa_dfs)
    except:
        # temporary solution for NOAA ftp not including 2024
        noaa_df = pd.DataFrame(index=pd.date_range(start='2024-01-01', periods=10, freq='H'))
        noaa_df['conditions'] = None
        noaa_df['airTemp_F'] = None
        noaa_df['dewPoint_F'] = None
        for station_name in station_names:
            formatted_dfs[station_name] = noaa_df
        print("Unable to collect NOAA data for timeframe")
    return formatted_dfs


def _format_df(station_ids: dict, noaa_dfs: dict) -> dict:
    """
    Function will take a list of station ids and a dictionary of filename and the respective file stored in a dataframe. 
    The function will return a dictionary where the key is the station id and the value is a dataframe for that station.

    Args: 
        station_ids (dict): Dictionary of station_ids,
        noaa_dfs (dict): dictionary of filename and the respective file stored in a dataframe
    Returns: 
        dict: Dictionary where the key is the station id and the value is a dataframe for that station
    """
    formatted_dfs = {}
    for value1 in station_ids.keys():
        # Append all DataFrames with the same station_id
        temp_df = pd.DataFrame(columns=['year', 'month', 'day', 'hour', 'airTemp', 'dewPoint',
                               'seaLevelPressure', 'windDirection', 'windSpeed', 'conditions', 'precip1Hour', 'precip6Hour'])
        for key, value in noaa_dfs.items():
            if key.startswith(value1):
                temp_df = pd.concat([temp_df, value], ignore_index=True)

        # Do unit Conversions
        # Convert all -9999 into N/A
        temp_df = temp_df.replace(-9999, np.NaN)

        # Convert tz from UTC to PT and format: Y-M-D HR:00:00
        temp_df["time"] = pd.to_datetime(
            temp_df[["year", "month", "day", "hour"]])
        temp_df["time"] = temp_df["time"].dt.tz_localize("UTC").dt.tz_convert('US/Pacific')

        # Convert airtemp, dewpoint, sealevelpressure, windspeed
        temp_df["airTemp_F"] = temp_df["airTemp"].apply(temp_c_to_f)
        temp_df["dewPoint_F"] = temp_df["dewPoint"].apply(temp_c_to_f)
        temp_df["seaLevelPressure_mb"] = temp_df["seaLevelPressure"].apply(
            divide_num_by_ten)
        temp_df["windSpeed_kts"] = temp_df["windSpeed"].apply(
            windspeed_mps_to_knots)

        # Convert precip
        temp_df["precip1Hour_mm"] = temp_df["precip1Hour"].apply(
            precip_cm_to_mm)
        temp_df["precip6Hour_mm"] = temp_df["precip6Hour"].apply(
            precip_cm_to_mm)

        # Match case conditions
        temp_df["conditions"] = temp_df["conditions"].apply(
            conditions_index_to_desc)

        # Rename windDirections
        temp_df["windDirection_deg"] = temp_df["windDirection"]

        # Drop columns that were replaced
        temp_df = temp_df.drop(["airTemp", "dewPoint", "seaLevelPressure", "windSpeed", "precip1Hour",
                               "precip6Hour", "year", "month", "day", "hour", "windDirection"], axis=1)

        temp_df.set_index(["time"], inplace=True)
        # Save df in dict
        formatted_dfs[station_ids[value1]] = temp_df

    return formatted_dfs


def _get_noaa_dictionary(weather_directory : str) -> dict:
    """
    This function downloads a dictionary of equivalent station id for each station name

    Args: 
        weather_directory : str 
            the directory that holds NOAA weather data files. Should not contain an ending "/" (e.g. "full/path/to/pipeline/data/weather")

    Returns: 
        dict: Dictionary of station id and corrosponding station name
    """

    if not os.path.isdir(weather_directory):
        os.makedirs(weather_directory)

    filename = "isd-history.csv"
    hostname = f"ftp.ncdc.noaa.gov"
    wd = f"/pub/data/noaa/"
    try:
        ftp_server = FTP(hostname)
        ftp_server.login()
        ftp_server.cwd(wd)
        ftp_server.encoding = "utf-8"
        with open(f"{weather_directory}/{filename}", "wb") as file:
            ftp_server.retrbinary(f"RETR {filename}", file.write)
        ftp_server.quit()
    except:
        print("FTP ERROR: Could not download weather dictionary")

    isd_directory = f"{weather_directory}/isd-history.csv"
    if not os.path.exists(isd_directory):
        print(f"File path '{isd_directory}' does not exist.")
        sys.exit()

    isd_history = pd.read_csv(isd_directory, dtype=str)
    isd_history["USAF_WBAN"] = isd_history['USAF'].str.cat(
        isd_history['WBAN'], sep="-")
    df_id_usafwban = isd_history[["ICAO", "USAF_WBAN"]]
    df_id_usafwban = df_id_usafwban.drop_duplicates(
        subset=["ICAO"], keep='first')
    noaa_dict = df_id_usafwban.set_index('ICAO').to_dict()['USAF_WBAN']
    return noaa_dict


def _download_noaa_data(stations: dict, weather_directory : str) -> List[str]:
    """
    This function takes in a list of the stations and downloads the corrosponding NOAA weather data via FTP and returns it in a List of filenames

    Args: 
        stations : dict)
            dictionary of station_ids who's data needs to be downloaded
        weather_directory : str 
            the directory that holds NOAA weather data files. Should not contain an ending "/" (e.g. "full/path/to/pipeline/data/weather")
    Returns: 
        List[str]: List of filenames that were downloaded
    """
    noaa_filenames = list()
    year_end = datetime.today().year

    try:
        hostname = f"ftp.ncdc.noaa.gov"
        ftp_server = FTP(hostname)
        ftp_server.login()
        ftp_server.encoding = "utf-8"
    except:
        print("FTP ERROR")
        return
    # Download files for each station from 2010 till present year
    for year in range(2010, year_end + 1):
        # Set FTP credentials and connect
        wd = f"/pub/data/noaa/isd-lite/{year}/"
        ftp_server.cwd(wd)
        # Download all files and save as station_year.gz in /data/weather
        for station in stations.keys():
            if not os.path.isdir(f"{weather_directory}/{stations[station]}"):
                os.makedirs(f"{weather_directory}/{stations[station]}")
            filename = f"{station}-{year}.gz"
            noaa_filenames.append(filename)
            file_path = f"{weather_directory}/{stations[station]}/{filename}"
            # Do not download if the file already exists
            if (os.path.exists(file_path) == False) or (year == year_end):
                with open(file_path, "wb") as file:
                    ftp_server.retrbinary(f"RETR {filename}", file.write)
            else:
                print(file_path, " exists")
    ftp_server.quit()
    return noaa_filenames


def _convert_to_df(stations: dict, noaa_filenames: List[str], weather_directory : str) -> dict:
    """
    Gets the list of downloaded filenames and imports the files and converts it to a dictionary of DataFrames

    Args: 
        stations : dict 
            Dict of stations 
        noaa_filenames : List[str]
            List of downloaded filenames
        weather_directory : str 
            the directory that holds NOAA weather data files. Should not contain an ending "/" (e.g. "full/path/to/pipeline/data/weather")
    Returns: 
        dict: Dictionary where key is filename and value is dataframe for the file
    """
    noaa_dfs = []
    for station in stations.keys():
        for filename in noaa_filenames:
            table = _gz_to_df(
                f"{weather_directory}/{stations[station]}/{filename}")
            table.columns = ['year', 'month', 'day', 'hour', 'airTemp', 'dewPoint', 'seaLevelPressure',
                             'windDirection', 'windSpeed', 'conditions', 'precip1Hour', 'precip6Hour']
            noaa_dfs.append(table)
    noaa_dfs_dict = dict(zip(noaa_filenames, noaa_dfs))
    return noaa_dfs_dict


def _gz_to_df(filename: str) -> pd.DataFrame:
    """
    Opens the file and returns it as a pd.DataFrame

    Args: 
        filename (str): String of filename to be converted
    Returns: 
        pd.DataFrame: DataFrame of the corrosponding file
    """
    with gzip.open(filename) as data:
        table = pd.read_table(data, header=None, delim_whitespace=True)
    return table
