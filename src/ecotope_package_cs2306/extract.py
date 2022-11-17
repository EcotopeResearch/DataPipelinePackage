from typing import List
import pandas as pd
from ftplib import FTP
from datetime import datetime
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
            with open(f"{filename}", "wb") as file:
                ftp_server.retrbinary(f"RETR {filename}", file.write)
        ftp_server.quit()

def main():
    stations = ["727935-24234"]
    get_noaa_data(stations)

main()