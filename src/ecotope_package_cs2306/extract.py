from typing import List
import pandas as pd
from ftplib import FTP
from datetime import datetime
import gzip
import os

def get_noaa_data(station_names: List[str]):
    noaa_dictionary = get_noaa_dictionary()
    station_ids = [noaa_dictionary[station_name][0] for station_name in station_names if station_name in noaa_dictionary]
    print(station_ids)
    noaa_filenames = download_noaa_data(station_ids)
    print(noaa_filenames)
    noaa_dfs = convert_to_df(noaa_filenames)
    print(noaa_dfs)

def get_noaa_dictionary() -> dict:
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

def download_noaa_data(stations: List[str]) -> List[str]:
    noaa_filenames = list()
    year_end = datetime.today().year
    for year in range(2010, year_end+1):
        hostname = f"ftp.ncdc.noaa.gov"
        wd = f"/pub/data/noaa/isd-lite/{year}/"
        ftp_server = FTP(hostname)
        ftp_server.login()
        ftp_server.cwd(wd)
        ftp_server.encoding = "utf-8"
        for station in stations:
            filename = f"{station}-{year}.gz"
            noaa_filenames.append(filename)
            file_path = f"output/{filename}"
            if os.path.exists(file_path) == False:
                with open(file_path, "wb") as file:
                    ftp_server.retrbinary(f"RETR {filename}", file.write)
            else:
                print(file_path, " exists")
        ftp_server.quit()
    return noaa_filenames

def convert_to_df(noaa_filenames: List[str]) -> dict:
    noaa_dfs = []
    for filename in noaa_filenames:
        noaa_dfs.append(gz_to_df(filename))
    noaa_dfs_dict = dict(zip(noaa_filenames, noaa_dfs))
    return noaa_dfs_dict

def gz_to_df(filename: str) -> pd.DataFrame:
    with gzip.open(f"output/{filename}") as data:
        table = pd.read_table(data, header=None)
    return table

def main():
    stations = ["727935-24234"]
    #, 'KPWM', 'KSFO', 'KAVL'
    get_noaa_data(['KBFI'])

main()