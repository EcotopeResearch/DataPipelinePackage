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
  file_server_path = 'somedir/'
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
      data = pd.read_json(file, lines=True)
      temp_dfs.append(data)
    
    # concatenate all dataframes into one dataframe 
    df = pd.concat(temp_dfs, ignore_index=True)
    return df

def get_noaa_data(station_names: List[str]) -> dict:
    noaa_dictionary = _get_noaa_dictionary()
    station_ids = [noaa_dictionary[station_name][0] for station_name in station_names if station_name in noaa_dictionary]
    noaa_filenames = _download_noaa_data(station_ids)
    noaa_dfs = _convert_to_df(noaa_filenames)
    print(noaa_dfs)

def _format_df(station_ids: dict, noaa_dfs: dict):
    formatted_dfs = {} 
    for key1, value1 in station_ids:
        # Append all DataFrames with the same station_id 
        temp_df = pd.DataFrame(columns = ['year','month','day','hour','airTemp','dewPoint','seaLevelPressure','windDirection','windSpeed','conditions','precip1Hour','precip6Hour'])
        for key, value in noaa_dfs.items():
            if key.startswith(value1):
                temp_df = pd.concat([temp_df, value], ignore_index=True)

        # Do unit Conversions

        # Convert all -9999 into N/A
        temp_df = temp_df.replace(-9999, np.NaN)

        # Convert tz from UTC to PT and format: Y-M-D HR:00:00
        #temp_df["time"] = 

        # Convert airtemp, dewpoint, sealevelpressure, windspeed
        temp_df["airTemp_F"] = temp_df["airTemp"].apply(unit_convert.temp_c_to_f)
        temp_df["dewPoint_F"] = temp_df["dewPoint"].apply(unit_convert.temp_c_to_f)
        temp_df["seaLevelPressure_mb"] = temp_df["seaLevelPressure"].apply(unit_convert.divide_num_by_ten) 
        temp_df["windSpeed_kts"] = temp_df["windSpeed"].apply(unit_convert.windspeed_mps_to_knots)  
        
        # Convert precip
        temp_df["precip1Hour_mm"] = temp_df["precip1Hour"].apply(unit_convert.precip_cm_to_mm)
        temp_df["precip6Hour_mm"] = temp_df["precip6Hour"].apply(unit_convert.precip_cm_to_mm)
        
        # Match case wind_direction
        temp_df["windDirection_deg"] = temp_df["windDirection"].apply(unit_convert.winddirection_index_to_deg)

        # Drop columns that were replaced
        temp_df = temp_df.drop(["airTemp", "dewPoint", "seaLevelPressure", "windSpeed", "precip1Hour", "precip6Hour"], axis = 1)
        
        # Save df in dict
        formatted_dfs[key1] = temp_df

    return formatted_dfs

def _get_noaa_dictionary() -> dict:
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
    noaa_filenames = list()
    year_end = datetime.today().year
    # Download files for each station from 2010 till present year
    for year in range(2010, year_end+1):
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
            # Do not download if the file already exists
            if os.path.exists(file_path) == False:
                with open(file_path, "wb") as file:
                    ftp_server.retrbinary(f"RETR {filename}", file.write)
            else:
                print(file_path, " exists")
        ftp_server.quit()
    return noaa_filenames

def _convert_to_df(noaa_filenames: List[str]) -> dict:
    # Gets the list of downloaded filenames and imports the files
    # and converts it to a dictionary of DataFrames
    noaa_dfs = []
    for filename in noaa_filenames:
        noaa_dfs.append(_gz_to_df(filename))
    noaa_dfs_dict = dict(zip(noaa_filenames, noaa_dfs))
    return noaa_dfs_dict

def _gz_to_df(filename: str) -> pd.DataFrame:
    # Opens the file and returns it as a pd.DataFrame
    with gzip.open(f"output/{filename}") as data:
        table = pd.read_table(data, header=None)
    return table

def __main__():
    stations = ["727935-24234"]
    #, 'KPWM', 'KSFO', 'KAVL'
    get_noaa_data(['KBFI'])


__main__()