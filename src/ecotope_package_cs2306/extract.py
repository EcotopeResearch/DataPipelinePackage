from asyncio.windows_events import NULL
from typing import List
import pandas as pd
from ftplib import FTP
from datetime import datetime
import gzip
import shutil

def get_noaa_data(stations: List[str]):
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
            with open(f"output/{filename}", "wb") as file:
                ftp_server.retrbinary(f"RETR {filename}", file.write)
                ftp_server.quit()
                unzip_gz(filename)
                #STOP
        ftp_server.quit()

def unzip_gz(filename):
    with gzip.open("output/727935-24234-2010.gz") as data:
        table = pd.read_table(data, header=None)
        print(table)

def main():
    stations = ["727935-24234"]
    #get_noaa_data(stations)
    unzip_gz("potato")

main()