from typing import List
import pandas as pd
from ftplib import FTP
from datetime import datetime
import gzip
import os, json
import re
import unit_convert
import numpy as np

# grabs all json files from file server and stores the paths to files in a list
def extract_json() -> List[str]:
  # path to file server currently unknown, will be updated later
  file_server_path = 'input/'
  json_filenames = []
  # find all json files in file server and append the file's full path to list
  for file in os.listdir(file_server_path):
    if file.endswith('.json'):
      full_filename = os.path.join(file_server_path, file)
      json_filenames.append(full_filename)
  
  # return list of json file paths 
  return json_filenames


# reads all json files into a singular dataframe
def json_to_df(json_filenames: List[str]) -> pd.DataFrame:
    temp_dfs = []
    # read each json file into dataframe and append to temporary list
    for file in json_filenames:
        test = json.load(open(file))
        data = pd.json_normalize(test, record_path=['sensors'], meta=['device', 'connection', 'time'])
        data["time"] = pd.to_datetime(data["time"])
        data["time"] = data["time"].dt.tz_localize("UTC").dt.tz_convert('US/Pacific')
        temp_dfs.append(data)

    # concatenate all dataframes into one dataframe 
    df = pd.concat(temp_dfs, ignore_index=True)
    return df

# merges the sensor and weather data
def merge_noaa(site: pd.DataFrame) -> pd.DataFrame:
    df = []
    data = pd.read_csv('output/727935-24234.csv', parse_dates=['time'])
    data["time"] = pd.to_datetime(data["time"], utc=True)
    data["time"] = data["time"].dt.tz_convert('US/Pacific')
    df = site.merge(data, how='left', on='time')
    return df

def get_noaa_data(station_names: List[str]) -> dict:
    """
    Function will take in a list of station names and will return a dictionary where the key is the station name and the 
    value is a dataframe with the parsed weather data
    Input: List of Station Names
    Output: Dictionary with key as Station Name and Value as DF of Parsed Weather Data
    """
    noaa_dictionary = _get_noaa_dictionary()
    print(noaa_dictionary)
    station_ids = [noaa_dictionary[station_name][0] for station_name in station_names if station_name in noaa_dictionary]
    noaa_filenames = _download_noaa_data(station_ids)
    noaa_dfs = _convert_to_df(noaa_filenames)
    formatted_dfs = _format_df(station_ids, noaa_dfs)
    return formatted_dfs

def _format_df(station_ids: list, noaa_dfs: dict) -> dict:
    """
    Function will take a list of station ids and a dictionary of filename and the respective file stored in a dataframe
    The function will return a dictionary where the key is the station id and the value is a dataframe for that station
    Input: List of station_ids, dictionary of filename and the respective file stored in a dataframe
    Output: Dictionary where the key is the station id and the value is a dataframe for that station
    """
    formatted_dfs = {}
    for value1 in station_ids:
        # Append all DataFrames with the same station_id 
        temp_df = pd.DataFrame(columns = ['year','month','day','hour','airTemp','dewPoint','seaLevelPressure','windDirection','windSpeed','conditions','precip1Hour','precip6Hour'])
        for key, value in noaa_dfs.items():
            if key.startswith(value1):
                temp_df = pd.concat([temp_df, value], ignore_index=True)

        print(temp_df)
        # Do unit Conversions
        print("Doing Conversions")
        # Convert all -9999 into N/A
        temp_df = temp_df.replace(-9999, np.NaN)

        # Convert tz from UTC to PT and format: Y-M-D HR:00:00
        temp_df["time"] = pd.to_datetime(temp_df[["year", "month", "day", "hour"]])
        temp_df["time"] = temp_df["time"].dt.tz_localize("UTC").dt.tz_convert('US/Pacific')

        # Convert airtemp, dewpoint, sealevelpressure, windspeed
        temp_df["airTemp_F"] = temp_df["airTemp"].apply(unit_convert.temp_c_to_f)
        temp_df["dewPoint_F"] = temp_df["dewPoint"].apply(unit_convert.temp_c_to_f)
        temp_df["seaLevelPressure_mb"] = temp_df["seaLevelPressure"].apply(unit_convert.divide_num_by_ten) 
        temp_df["windSpeed_kts"] = temp_df["windSpeed"].apply(unit_convert.windspeed_mps_to_knots)  
        
        # Convert precip
        temp_df["precip1Hour_mm"] = temp_df["precip1Hour"].apply(unit_convert.precip_cm_to_mm)
        temp_df["precip6Hour_mm"] = temp_df["precip6Hour"].apply(unit_convert.precip_cm_to_mm)
        
        # Match case conditions
        temp_df["conditions"] = temp_df["conditions"].apply(unit_convert.conditions_index_to_desc)

        # Rename windDirections
        temp_df["windDirection_deg"] = temp_df["windDirection"]

        # Drop columns that were replaced
        temp_df = temp_df.drop(["airTemp", "dewPoint", "seaLevelPressure", "windSpeed", "precip1Hour", "precip6Hour", "year", "month", "day", "hour", "windDirection"], axis = 1)
        
        print(temp_df)
        # Save df in dict
        formatted_dfs[value1] = temp_df

    return formatted_dfs

def _get_noaa_dictionary() -> dict:
    """
    This function downloads a dictionary of equivalent station id for each station name
    Input: None
    Output: Dictionary of station id and corrosponding station name
    """
    filename = "isd-history.csv"
    hostname = f"ftp.ncdc.noaa.gov"
    wd = f"/pub/data/noaa/"
    ftp_server = FTP(hostname)
    ftp_server.login()
    ftp_server.cwd(wd)
    ftp_server.encoding = "utf-8"
    with open(f"output/{filename}", "wb") as file:
        ftp_server.retrbinary(f"RETR {filename}", file.write)
    ftp_server.quit()
    isd_history = pd.read_csv(f"output/isd-history.csv",dtype=str)
    isd_history["USAF_WBAN"] = isd_history['USAF'].str.cat(
        isd_history['WBAN'], sep ="-")
    df_id_usafwban = isd_history[["ICAO", "USAF_WBAN"]]
    df_id_usafwban = df_id_usafwban.drop_duplicates(subset = ["ICAO"], keep = 'first')
    return df_id_usafwban.set_index("ICAO").T.to_dict('list')

def _download_noaa_data(stations: List[str]) -> List[str]:
    """
    This function takes in a list of the stations and downloads the corrosponding NOAA weather data
    via FTP and returns it in a List of filenames
    Input: List of station_ids who's data needs to be downloaded
    Output: List of filenames that were downloaded
    """
    noaa_filenames = list()
    year_end = datetime.today().year
    # Download files for each station from 2010 till present year
    for year in range(2010, year_end + 1):
        # Set FTP credentials and connect
        hostname = f"ftp.ncdc.noaa.gov"
        wd = f"/pub/data/noaa/isd-lite/{year}/"
        ftp_server = FTP(hostname)
        ftp_server.login()
        ftp_server.cwd(wd)
        ftp_server.encoding = "utf-8"
        # Download all files and save as station_year.gz in /output
        for station in stations:
            filename = f"{station}-{year}.gz"
            noaa_filenames.append(filename)
            file_path = f"output/{filename}"
            # Do not download if the file already exists (FIX: Needsto redownload most recent year every time)
            if os.path.exists(file_path) == False:
                with open(file_path, "wb") as file:
                    ftp_server.retrbinary(f"RETR {filename}", file.write)
            else:
                print(file_path, " exists")
        ftp_server.quit()
    return noaa_filenames

def _convert_to_df(noaa_filenames: List[str]) -> dict:
    """
    Gets the list of downloaded filenames and imports the files
    and converts it to a dictionary of DataFrames
    Input: List of downloaded filenames
    Output: Dictionary where key is filename and value is dataframe for the file
    """
    noaa_dfs = []
    for filename in noaa_filenames:
        noaa_dfs.append(_gz_to_df(filename))
    noaa_dfs_dict = dict(zip(noaa_filenames, noaa_dfs))
    return noaa_dfs_dict

def _gz_to_df(filename: str) -> pd.DataFrame:
    """
    Opens the file and returns it as a pd.DataFrame
    Input: String of filename to be converted
    Output: DataFrame of the corrosponding file
    """
    with gzip.open(f"output/{filename}") as data:
        table = pd.read_table(data, header=None, delim_whitespace=True)
    table.columns = ['year','month','day','hour','airTemp','dewPoint','seaLevelPressure','windDirection','windSpeed','conditions','precip1Hour','precip6Hour']
    return table

def __main__():
    """""
    stations = ["727935-24234"]
    #, 'KPWM', 'KSFO', 'KAVL'
    formatted_dfs = get_noaa_data(['KBFI'])
    print(formatted_dfs)
    for key, value in formatted_dfs.items():
        value.to_csv(f"output/{key}.csv", index=False)
        print("done 1")
    print("done")
    """""

if __name__ == '__main__':
    __main__()