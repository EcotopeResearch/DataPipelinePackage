import pandas as pd
import requests
import traceback
from ecopipeline import ConfigManager
from ecopipeline.extract.APIExtractor import APIExtractor
from datetime import datetime, timedelta


class LiCOR(APIExtractor):
    """APIExtractor for the LI-COR Cloud API.

    Queries sensor data for the configured device between start_time and end_time.
    Returns a DataFrame indexed by UTC timestamp with sensor serial numbers as columns.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 create_csv: bool = True, csv_prefix: str = ""):
        super().__init__(config, start_time, end_time, create_csv, csv_prefix)

    def raw_data_to_df(self, config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
        if endTime is None:
            endTime = datetime.now()
        if startTime is None:
            startTime = endTime - timedelta(hours=28)

        url = 'https://api.licor.cloud/v2/data'
        params = {
            'deviceSerialNumber': config.api_device_id,
            'startTime': f'{int(startTime.timestamp()) * 1000}',
            'endTime': f'{int(endTime.timestamp()) * 1000}',
        }
        headers = {
            'accept': 'application/json',
            'Authorization': f'Bearer {config.api_token}',
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                response_json = response.json()
                data = {}
                if 'sensors' in response_json:
                    for sensor in response_json['sensors']:
                        sensor_id = sensor['sensorSerialNumber']
                        for measurement in sensor.get('data', []):
                            try:
                                records = measurement.get('records', [])
                                data[sensor_id] = pd.Series(
                                    {record[0]: self._get_float_value(record[1]) for record in records}
                                )
                            except Exception:
                                print(f"Could not convert {sensor_id} values to floats.")
                df = pd.DataFrame(data)
                df.index = pd.to_datetime(df.index, unit='ms')
                df = df.sort_index()
                return df
            else:
                print(f"Failed to make GET request. Status code: {response.status_code} {response.json()}")
                return pd.DataFrame()
        except Exception as e:
            traceback.print_exc()
            print(f"An error occurred: {e}")
            return pd.DataFrame()
