from typing import List
import pandas as pd
import re
from ftplib import FTP
from datetime import datetime, timedelta
import gzip
import os
import json
from ecopipeline.unit_convert import temp_c_to_f, divide_num_by_ten, windspeed_mps_to_knots, precip_cm_to_mm, conditions_index_to_desc
from ecopipeline.load import connect_db, get_login_info
from ecopipeline.config import _config_directory, _data_directory
import numpy as np
import sys
from pytz import timezone, utc
import mysql.connector.errors as mysqlerrors


def get_last_full_day_from_db() -> datetime:
    """
    Function retrieves the last line from the database with the most recent datetime 
    in local time.
    
    Args:
        None
    Returns: 
        datetime: end of last full day populated in database or default past time if no data found
    """
    config_dict = get_login_info(["minute"])
    db_connection, db_cursor = connect_db(config_info=config_dict['database'])
    return_time = datetime(year=2000, month=1, day=9, hour=23, minute=59, second=0).astimezone(timezone('US/Pacific')) # arbitrary default time
    
    try:
        db_cursor.execute(
            f"select * from {config_dict['minute']['table_name']} order by time_pt DESC LIMIT 1")

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

def get_db_row_from_time(time: datetime) -> pd.DataFrame:
    """
    Extracts a row from the applicable minute table in the database for the given datetime or returns empty dataframe if none exists

    Args:
        time (datetime): The time index to get the row from
    Returns: 
        pd.DataFrame: Pandas Dataframe containing the row or empty if no row exists for the timestamp
    """
    config_dict = get_login_info(["minute"])
    db_connection, db_cursor = connect_db(config_info=config_dict['database'])
    row_data = pd.DataFrame()

    try:
        db_cursor.execute(
            f"SELECT * FROM {config_dict['minute']['table_name']} WHERE time_pt = '{time}'")
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

def extract_new(startTime: datetime, filenames: List[str], decihex = False, timeZone: str = None, endTime: datetime = None) -> List[str]:
    """
    Function filters the filenames to only those newer than the last date in database.
    If filenames are in deciheximal, convert them to datetime. Note that for some projects,
    files are dropped at irregular intervals so data cannot be filtered by exact date. 

    Args: 
        time (datetime) : The point in time for which we want to start the data extraction from. This 
            is local time from the data's index. 
        filenames (List[str]) : List of filenames to be filtered
        decihex (bool) : Whether or not the filenames need to be converted to datetime from deciheximal
        timeZone (str): Local time zone. 
    Returns: 
        List[str]: Filtered list of filenames
    """
    
    if decihex: 
        base_date = datetime(1970, 1, 1)
        file_dates = [pd.Timestamp(base_date + timedelta(seconds = int(re.search(r'\.(.*?)_', filename).group(1), 16))) for filename in filenames] #convert decihex to dates, these are in utc
        file_dates_local = [file_date.tz_localize('UTC').tz_convert(timezone(timeZone)).tz_localize(None) for file_date in file_dates] #convert utc to local zone with no awareness
        
        return_list = [filename for filename, local_time in zip(filenames, file_dates_local) if local_time > startTime and (endTime is None or local_time < endTime)]


    else: 
        startTime_int = int(startTime.strftime("%Y%m%d%H%M%S"))
        return_list = list(filter(lambda filename: int(filename[-17:-3]) >= startTime_int and (endTime is None or int(filename[-17:-3]) < int(endTime.strftime("%Y%m%d%H%M%S"))), filenames))
    return return_list

def extract_files(extension: str, subdir: str = "") -> List[str]:
    """
    Function takes in a file extension and subdirectory and returns a list of paths files in the directory of that type.

    Args: 
        extension (str): File extension as string
        subdir (str): subdirectory (defaults to no subdir)
    Returns: 
        List[str]: List of filenames 
    """
    os.chdir(os.getcwd())
    filenames = []
    for file in os.listdir(os.path.join(_data_directory, subdir)):
        if file.endswith(extension):
            full_filename = os.path.join(_data_directory, subdir, file)
            filenames.append(full_filename)

    return filenames


def json_to_df(json_filenames: List[str], time_zone: str = 'US/Pacific') -> pd.DataFrame:
    """
    Function takes a list of gz/json filenames and reads all files into a singular dataframe.

    Args: 
        json_filenames (List[str]): List of filenames 
        time_zone (str): Local timezone, default is US/Pacific
    Returns: 
        pd.DataFrame: Pandas Dataframe containing data from all files
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
            temp_dfs.append(norm_data)

    df = pd.concat(temp_dfs, ignore_index=False)
    return df


def csv_to_df(csv_filenames: List[str]) -> pd.DataFrame:
    """
    Function takes a list of csv filenames and reads all files into a singular dataframe.

    Args: 
        csv_filenames (List[str]): List of filenames 
    Returns: 
        pd.DataFrame: Pandas Dataframe containing data from all files
    """
    temp_dfs = []
    for file in csv_filenames:
        try:
            data = pd.read_csv(file)
        except FileNotFoundError:
            print("File Not Found: ", file)
            return

        if len(data) != 0:
            temp_dfs.append(data)
    df = pd.concat(temp_dfs, ignore_index=False)
    return df


def get_sub_dirs(dir: str) -> List[str]:
    """
    Function takes in a directory and returns a list of the paths to all immediate subfolders in that directory. 
    This is used when multiple sites are being ran in same pipeline. 

    Args: 
        dir (str): Directory as a string.
    Returns: 
        List[str]: List of paths to subfolders.
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


def get_noaa_data(station_names: List[str]) -> dict:
    """
    Function will take in a list of station names and will return a dictionary where the key is the station name and the value is a dataframe with the parsed weather data.

    Args: 
        station_names (List[str]): List of Station Names
    Returns: 
        dict: Dictionary with key as Station Name and Value as DF of Parsed Weather Data
    """
    noaa_dictionary = _get_noaa_dictionary()
    station_ids = {noaa_dictionary[station_name]
        : station_name for station_name in station_names if station_name in noaa_dictionary}
    noaa_filenames = _download_noaa_data(station_ids)
    noaa_dfs = _convert_to_df(station_ids, noaa_filenames)
    formatted_dfs = _format_df(station_ids, noaa_dfs)
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


def _get_noaa_dictionary() -> dict:
    """
    This function downloads a dictionary of equivalent station id for each station name

    Args: 
        None
    Returns: 
        dict: Dictionary of station id and corrosponding station name
    """

    if not os.path.isdir(f"{_data_directory}weather"):
        os.makedirs(f"{_data_directory}weather")

    filename = "isd-history.csv"
    hostname = f"ftp.ncdc.noaa.gov"
    wd = f"/pub/data/noaa/"
    try:
        ftp_server = FTP(hostname)
        ftp_server.login()
        ftp_server.cwd(wd)
        ftp_server.encoding = "utf-8"
        with open(f"{_data_directory}weather/{filename}", "wb") as file:
            ftp_server.retrbinary(f"RETR {filename}", file.write)
        ftp_server.quit()
    except:
        print("FTP ERROR")

    isd_directory = f"{_data_directory}weather/isd-history.csv"
    if not os.path.exists(isd_directory):
        print(
            f"File path '{isd_directory}' does not exist.")
        sys.exit()

    isd_history = pd.read_csv(isd_directory, dtype=str)
    isd_history["USAF_WBAN"] = isd_history['USAF'].str.cat(
        isd_history['WBAN'], sep="-")
    df_id_usafwban = isd_history[["ICAO", "USAF_WBAN"]]
    df_id_usafwban = df_id_usafwban.drop_duplicates(
        subset=["ICAO"], keep='first')
    noaa_dict = df_id_usafwban.set_index('ICAO').to_dict()['USAF_WBAN']
    return noaa_dict


def _download_noaa_data(stations: dict) -> List[str]:
    """
    This function takes in a list of the stations and downloads the corrosponding NOAA weather data via FTP and returns it in a List of filenames

    Args: 
        stations (dict): dictionary of station_ids who's data needs to be downloaded
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
            if not os.path.isdir(f"{_data_directory}weather/{stations[station]}"):
                os.makedirs(f"{_data_directory}weather/{stations[station]}")
            filename = f"{station}-{year}.gz"
            noaa_filenames.append(filename)
            file_path = f"{_data_directory}weather/{stations[station]}/{filename}"
            # Do not download if the file already exists
            if (os.path.exists(file_path) == False) or (year == year_end):
                with open(file_path, "wb") as file:
                    ftp_server.retrbinary(f"RETR {filename}", file.write)
            else:
                print(file_path, " exists")
    ftp_server.quit()
    return noaa_filenames


def _convert_to_df(stations: dict, noaa_filenames: List[str]) -> dict:
    """
    Gets the list of downloaded filenames and imports the files and converts it to a dictionary of DataFrames

    Args: 
        stations (dict): Dict of stations 
        noaa_filenames (List[str]): List of downloaded filenames
    Returns: 
        dict: Dictionary where key is filename and value is dataframe for the file
    """
    noaa_dfs = []
    for station in stations.keys():
        for filename in noaa_filenames:
            table = _gz_to_df(
                f"{_data_directory}weather/{stations[station]}/{filename}")
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
