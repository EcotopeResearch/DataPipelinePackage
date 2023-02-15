from typing import List
import pandas as pd
from ftplib import FTP
from datetime import datetime
import gzip
import os, json
import datetime as dt
from ecotope_package_cs2306.unit_convert import temp_c_to_f, divide_num_by_ten, windspeed_mps_to_knots, precip_cm_to_mm, conditions_index_to_desc
from ecotope_package_cs2306.load import connectDB, getLoginInfo
from ecotope_package_cs2306.config import _config_directory, _data_directory, _output_directory
import numpy as np
import sys
import mysql.connector.errors as mysqlerrors

def get_last_line(config_file_path: str = _config_directory) -> pd.DataFrame:
    """
    Function retrieves the last line from the database with the most recent datetime.
    Input: Path to config file (defaulr is set in load.py)
    Output: One line DF of the last entry
    """
    config_dict = getLoginInfo(config_file_path)
    db_connection, db_cursor = connectDB(config_info=config_dict['database'])

    try:
        db_cursor.execute(f"select * from {config_dict['minute']['table_name']} order by time DESC LIMIT 1")
    except mysqlerrors.Error:
        print(f"Table {config_dict['minute']['table_name']} does not exist.")
        return 0

    last_row_data = pd.DataFrame(db_cursor.fetchall())
    last_time = last_row_data[0][0]

    if ((last_time.hour != 23) and (last_time.minute != 59)):
        last_full_day = datetime(year=last_time.year, month=last_time.month, day=last_time.day-1, hour=23, minute=59, second=0)
        try:
            db_cursor.execute(f"select * from {config_dict['minute']['table_name']} where time = '{last_full_day}'")
        except mysqlerrors.Error:
            print(f"Table {config_dict['minute']['table_name']} does not exist.")
            return 0

        last_row_data = pd.DataFrame(db_cursor.fetchall())

    try:
        db_cursor.execute(f"select column_name from information_schema.columns where table_schema = '"
                        f"{config_dict['database']['database']}' and table_name = '"
                        f"{config_dict['minute']['table_name']}'")
    except mysqlerrors.Error:
        print(f"Table {config_dict['minute']['table_name']} does not exist.")
        return 0
    
    columns_names = db_cursor.fetchall()
    columns_names = [name[0] for name in columns_names]
    last_row_data.columns = columns_names
    last_row_data.set_index(last_row_data['time'], inplace=True)
    last_row_data.drop(['time'], axis=1, inplace=True)

    db_cursor.close()
    db_connection.close()

    return last_row_data

def extract_new(last_row: pd.DataFrame, json_filenames: List[str]) -> List[str]:
    """
    Function filters the filenames to only those newer than the last date.
    Input: The last row in the database, List of filenames to be filtered
    Output: Filtered list of filenames
    """
    time = last_row.squeeze()
    time = time.name
    time = time.to_pydatetime()
    time_int = int(time.strftime("%Y%m%d%H%M%S"))
    return list(filter(lambda filename: int(filename[-17:-3]) >= time_int, json_filenames))


def extract_files(extension : str) -> List[str]:
  """
  Function takes in a file extension and returns 
  a list of paths files in the directory of that type.
  Input: File extension as string
  Output: List of filenames 
  """
  os.chdir(os.getcwd())
  filenames = []
  for file in os.listdir(_data_directory):
    if file.endswith(extension):
      full_filename = os.path.join(_data_directory, file)
      filenames.append(full_filename)
  
  return filenames

def json_to_df(json_filenames: List[str]) -> pd.DataFrame:
    """
    Function takes a list of gz/json filenames and reads all files into a singular dataframe.
    Input: List of filenames 
    Output: Pandas Dataframe containing data from all files
    """
    temp_dfs = []
    # read each json file into dataframe and append to temporary list
    for file in json_filenames:
        data = gzip.open(file)
        data = json.load(data)
        # TODO: This section is BV specific, maybe move to another function
        norm_data = pd.json_normalize(data, record_path=['sensors'], meta=['device', 'connection', 'time'])
        norm_data["time"] = pd.to_datetime(norm_data["time"])
        norm_data["time"] = norm_data["time"].dt.tz_localize("UTC").dt.tz_convert('US/Pacific')
        norm_data = pd.pivot_table(norm_data, index="time", columns = "id", values = "data")
        temp_dfs.append(norm_data)

    # concatenate all dataframes into one dataframe 
    df = pd.concat(temp_dfs, ignore_index=False)
    return df


# TODO: This does nothing
# def merge_noaa(site: pd.DataFrame) -> pd.DataFrame:
#     """
#     Function takes a dataframe containing sensor data and merges it with weather data.
#     Input: Pandas Dataframe
#     Output: Merged Pandas Dataframe
#     """
#     df = []
#     data = pd.read_csv('output/727935-24234.csv', parse_dates=['time'])
#     data["time"] = pd.to_datetime(data["time"], utc=True)
#     data["time"] = data["time"].dt.tz_convert('US/Pacific')
#     df = site.merge(data, how='left', on='time')
#     return df

def get_noaa_data(station_names: List[str]) -> dict:
    """
    Function will take in a list of station names and will return a dictionary where 
    the key is the station name and the value is a dataframe with the parsed weather data.
    Input: List of Station Names
    Output: Dictionary with key as Station Name and Value as DF of Parsed Weather Data
    """
    noaa_dictionary = _get_noaa_dictionary()
    station_ids = {noaa_dictionary[station_name] : station_name for station_name in station_names if station_name in noaa_dictionary}
    noaa_filenames = _download_noaa_data(station_ids)
    noaa_dfs = _convert_to_df(station_ids, noaa_filenames)
    formatted_dfs = _format_df(station_ids, noaa_dfs)
    return formatted_dfs


def _format_df(station_ids: dict, noaa_dfs: dict) -> dict:
    """
    Function will take a list of station ids and a dictionary of filename and the respective file stored in a dataframe
    The function will return a dictionary where the key is the station id and the value is a dataframe for that station
    Input: List of station_ids, dictionary of filename and the respective file stored in a dataframe
    Output: Dictionary where the key is the station id and the value is a dataframe for that station
    """
    formatted_dfs = {}
    for value1 in station_ids.keys():
        # Append all DataFrames with the same station_id 
        temp_df = pd.DataFrame(columns = ['year','month','day','hour','airTemp','dewPoint','seaLevelPressure','windDirection','windSpeed','conditions','precip1Hour','precip6Hour'])
        for key, value in noaa_dfs.items():
            if key.startswith(value1):
                temp_df = pd.concat([temp_df, value], ignore_index=True)

        # Do unit Conversions
        # Convert all -9999 into N/A
        temp_df = temp_df.replace(-9999, np.NaN)

        # Convert tz from UTC to PT and format: Y-M-D HR:00:00
        temp_df["time"] = pd.to_datetime(temp_df[["year", "month", "day", "hour"]])
        temp_df["time"] = temp_df["time"].dt.tz_localize("UTC").dt.tz_convert('US/Pacific')

        # Convert airtemp, dewpoint, sealevelpressure, windspeed
        temp_df["airTemp_F"] = temp_df["airTemp"].apply(temp_c_to_f)
        temp_df["dewPoint_F"] = temp_df["dewPoint"].apply(temp_c_to_f)
        temp_df["seaLevelPressure_mb"] = temp_df["seaLevelPressure"].apply(divide_num_by_ten) 
        temp_df["windSpeed_kts"] = temp_df["windSpeed"].apply(windspeed_mps_to_knots)  
        
        # Convert precip
        temp_df["precip1Hour_mm"] = temp_df["precip1Hour"].apply(precip_cm_to_mm)
        temp_df["precip6Hour_mm"] = temp_df["precip6Hour"].apply(precip_cm_to_mm)
        
        # Match case conditions
        temp_df["conditions"] = temp_df["conditions"].apply(conditions_index_to_desc)

        # Rename windDirections
        temp_df["windDirection_deg"] = temp_df["windDirection"]

        # Drop columns that were replaced
        temp_df = temp_df.drop(["airTemp", "dewPoint", "seaLevelPressure", "windSpeed", "precip1Hour", "precip6Hour", "year", "month", "day", "hour", "windDirection"], axis = 1)
        
        temp_df.set_index(["time"], inplace=True)
        # Save df in dict
        formatted_dfs[station_ids[value1]] = temp_df

    return formatted_dfs


def _get_noaa_dictionary() -> dict:
    """
    This function downloads a dictionary of equivalent station id for each station name
    Input: None
    Output: Dictionary of station id and corrosponding station name
    """

    if not os.path.isdir(f"{_output_directory}weather"):
        os.makedirs(f"{_output_directory}weather")

    filename = "isd-history.csv"
    hostname = f"ftp.ncdc.noaa.gov"
    wd = f"/pub/data/noaa/"
    try:
        ftp_server = FTP(hostname)
        ftp_server.login()
        ftp_server.cwd(wd)
        ftp_server.encoding = "utf-8"
        with open(f"{_output_directory}weather/{filename}", "wb") as file:
            ftp_server.retrbinary(f"RETR {filename}", file.write)
        ftp_server.quit()
    except:
        print("FTP ERROR")

    isd_directory = f"{_output_directory}weather/isd-history.csv"
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
    This function takes in a list of the stations and downloads the corrosponding NOAA weather data
    via FTP and returns it in a List of filenames
    Input: List of station_ids who's data needs to be downloaded
    Output: List of filenames that were downloaded
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
    # Download files for each station from 2010 till present year
    for year in range(2010, year_end + 1):
        # Set FTP credentials and connect
        wd = f"/pub/data/noaa/isd-lite/{year}/"
        ftp_server.cwd(wd)
        # Download all files and save as station_year.gz in /output
        for station in stations.keys():
            if not os.path.isdir(f"{_output_directory}weather/{stations[station]}"):
                os.makedirs(f"{_output_directory}weather/{stations[station]}")
            filename = f"{station}-{year}.gz"
            noaa_filenames.append(filename)
            file_path = f"{_output_directory}weather/{stations[station]}/{filename}"
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
    Gets the list of downloaded filenames and imports the files
    and converts it to a dictionary of DataFrames
    Input: Dict of stations, List of downloaded filenames
    Output: Dictionary where key is filename and value is dataframe for the file
    """
    noaa_dfs = []
    for station in stations.keys():
        for filename in noaa_filenames:
            table = _gz_to_df(
                f"{_output_directory}weather/{stations[station]}/{filename}")
            table.columns = ['year','month','day','hour','airTemp','dewPoint','seaLevelPressure','windDirection','windSpeed','conditions','precip1Hour','precip6Hour']
            noaa_dfs.append(table)
    noaa_dfs_dict = dict(zip(noaa_filenames, noaa_dfs))
    return noaa_dfs_dict


def _gz_to_df(filename: str) -> pd.DataFrame:
    """
    Opens the file and returns it as a pd.DataFrame
    Input: String of filename to be converted
    Output: DataFrame of the corrosponding file
    """
    with gzip.open(filename) as data:
        table = pd.read_table(data, header=None, delim_whitespace=True)
    return table


def __main__():
    result_df = get_last_line()
    print(result_df)

if __name__ == '__main__':
    __main__()