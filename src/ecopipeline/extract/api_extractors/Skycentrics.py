import pandas as pd
import gzip
import json
import requests
from ecopipeline import ConfigManager
from ecopipeline.extract.APIExtractor import APIExtractor
from datetime import datetime, timedelta


class Skycentrics(APIExtractor):
    """APIExtractor for the Skycentrics API.

    Pulls data day-by-day between start_time and end_time, normalises the JSON
    sensor records into a pivot table, and rounds 59:59 timestamps up to the
    next minute.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 create_csv: bool = True, csv_prefix: str = "", time_zone: str = 'US/Pacific'):
        self.time_zone = time_zone
        super().__init__(config, start_time, end_time, create_csv, csv_prefix)

    def raw_data_to_df(self, config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
        if endTime is None:
            endTime = datetime.utcnow()
        if startTime is None:
            startTime = endTime - timedelta(days=1)

        temp_dfs = []
        time_parser = startTime
        while time_parser < endTime:
            time_parse_end = time_parser + timedelta(days=1)
            start_time_str = time_parser.strftime('%Y-%m-%dT%H:%M:%S')
            end_time_str = time_parse_end.strftime('%Y-%m-%dT%H:%M:%S')
            skycentrics_token, date_str = config.get_skycentrics_token(
                request_str=f'GET /api/devices/{config.api_device_id}/data?b={start_time_str}&e={end_time_str}&g=1 HTTP/1.1',
                date_str=None)
            response = requests.get(
                f'https://api.skycentrics.com/api/devices/{config.api_device_id}/data?b={start_time_str}&e={end_time_str}&g=1',
                headers={'Date': date_str, 'x-sc-api-token': skycentrics_token, 'Accept': 'application/gzip'})
            if response.status_code == 200:
                decompressed_data = gzip.decompress(response.content)
                json_data = json.loads(decompressed_data)
                norm_data = pd.json_normalize(json_data, record_path=['sensors'], meta=['time'], meta_prefix='response_')
                if len(norm_data) != 0:
                    norm_data['time_pt'] = pd.to_datetime(norm_data['response_time'], utc=True)
                    norm_data['time_pt'] = norm_data['time_pt'].dt.tz_convert(self.time_zone)
                    norm_data = pd.pivot_table(norm_data, index='time_pt', columns='id', values='data')
                    for i in range(len(norm_data.index)):
                        if norm_data.index[i].minute == 59 and norm_data.index[i].second == 59:
                            norm_data.index.values[i] = norm_data.index[i] + pd.Timedelta(seconds=1)
                    temp_dfs.append(norm_data)
            else:
                print(f"Failed to make GET request. Status code: {response.status_code} {response.json()}")
            time_parser = time_parse_end

        if len(temp_dfs) > 0:
            return pd.concat(temp_dfs, ignore_index=False)
        print("No Skycentrics data retrieved for time frame.")
        return pd.DataFrame()
