import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
from ecopipeline.extract.APIExtractor import APIExtractor
import requests
from datetime import datetime, timedelta
import os

class ThingsBoard(APIExtractor):
    def __init__(self, config : ConfigManager, start_time: datetime = None, end_time: datetime = None, create_csv : bool = True):
        self.device_id_overwrite = None
        self.sensor_keys = []
        self.query_hours = 1
        self.seperate_keys = False
        super().__init__(config, start_time, end_time, create_csv)
        
    def _get_tb_keys(self, token : str, api_device_id : str) -> list[str]:
        url = f'https://thingsboard.cloud/api/plugins/telemetry/DEVICE/{api_device_id}/keys/timeseries'

        # Headers
        headers = {
            'accept': 'application/json',
            'X-Authorization': f'Bearer {token}'
        }

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
                
            print(f"Failed to make GET request. Status code: {response.status_code} {response.json()}")
            return []
        except Exception as e:
            print(f"An error occurred: {e}")
            return []
        
    def raw_data_to_df(self, config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
        if endTime is None:
            endTime = datetime.now()
        if startTime is None:
            # 28 hours to ensure encapsulation of last day
            startTime = endTime - timedelta(hours=28)

        df = pd.DataFrame()
        api_device_id = self.device_id_overwrite if not self.device_id_overwrite is None else config.api_device_id
        if len(self.sensor_keys) <= 0:
            token = config.get_thingsboard_token()
            key_list = self._get_tb_keys(token, api_device_id)
            if len(key_list) <= 0:
                raise Exception(f"No sensors available at ThingsBoard site with id {api_device_id}")
            self.sensor_keys = key_list


        if endTime - timedelta(hours=self.query_hours) > startTime:
            time_diff = endTime - startTime
            midpointTime = startTime + time_diff / 2
            df_1 = self.raw_data_to_df(config, startTime, midpointTime)
            df_2 = self.raw_data_to_df(config, midpointTime, endTime)
            df = pd.concat([df_1, df_2])
            df = df.sort_index()
            df = df.groupby(df.index).mean()
        else:
            url = f'https://thingsboard.cloud/api/plugins/telemetry/DEVICE/{api_device_id}/values/timeseries'
            token = config.get_thingsboard_token()
            key_string = ','.join(self.sensor_keys)
            params = {
                'keys': key_string,
                'startTs': f'{int(startTime.timestamp())*1000}',
                'endTs': f'{int(endTime.timestamp())*1000}',
                'orderBy': 'ASC',
                'useStrictDataTypes': 'false',
                'interval' : '0',
                'agg' : 'NONE'
            }
            # Headers
            headers = {
                'accept': 'application/json',
                'X-Authorization': f'Bearer {token}'
            }

            try:
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    response_json = response.json()
                        
                    data = {}
                    for key, records in response_json.items():
                        try:
                            series = pd.Series(
                                data={record['ts']: self._get_float_value(record['value'])  for record in records}
                            )
                            data[key] = series
                        except:
                            print_statement = f"Could not convert {key} values to floats."
                            print(print_statement)
                    df = pd.DataFrame(data)
                    df.index = pd.to_datetime(df.index, unit='ms')
                    df = df.sort_index()
                else:
                    print(f"Failed to make GET request. Status code: {response.status_code} {response.json()}")
                    df = pd.DataFrame()
            except Exception as e:
                print(f"An error occurred: {e}")
                df = pd.DataFrame()
        return df