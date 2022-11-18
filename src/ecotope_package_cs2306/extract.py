from typing import List
import pandas as pd
from ftplib import FTP
from datetime import datetime
import gzip

def get_noaa_data(stations: List[str]):
    noaa_filenames = download_noaa_data(stations)
    noaa_dfs = convert_to_df(noaa_filenames)

def download_noaa_data(stations: List[str]) -> List[str]:
    noaa_filenames = List()
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
            with open(f"output/{filename}", "wb") as file:
                ftp_server.retrbinary(f"RETR {filename}", file.write)
                ftp_server.quit()
                #STOP
        ftp_server.quit()
    return noaa_filenames

def convert_to_df(noaa_filenames: List[str]) -> List[pd.DataFrame]:
    noaa_dfs = List()
    for filename in noaa_filenames:
        noaa_dfs.append(gz_to_df(filename))
    return noaa_dfs
        

def gz_to_df(filename: str) -> pd.DataFrame:
    with gzip.open(f"output/{filename}") as data:
        table = pd.read_table(data, header=None)
    return table

def main():
    stations = ["727935-24234"]
    #get_noaa_data(stations)
    unzip_gz("potato")

main()