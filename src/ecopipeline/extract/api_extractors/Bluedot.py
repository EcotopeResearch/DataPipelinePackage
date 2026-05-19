import pandas as pd
import requests
import traceback
from ecopipeline import ConfigManager
from ecopipeline.extract.APIExtractor import APIExtractor
from datetime import datetime, timedelta


BASE_URL = 'https://prod.bluebot.com/flow/v2'


class Bluedot(APIExtractor):
    """APIExtractor for the BlueBot Flow API.

    Queries sensor data for the configured device between ``start_time`` and
    ``end_time``.

    The API key stored in ``config.api_token`` is expected in ``PREFIX.VALUE``
    format.  Only the portion after the dot is sent in the authorization header
    as ``bluebot-api-key VALUE``.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the
        pipeline.  ``config.api_token`` must be set to the full API key
        (``PREFIX.VALUE`` form) obtained from the BlueBot portal.
    start_time : datetime, optional
        The start of the data extraction window.
    end_time : datetime, optional
        The end of the data extraction window. Defaults to ``datetime.now()``
        if not provided.
    create_csv : bool, optional
        If ``True``, writes the raw DataFrame to a CSV file in the configured
        data directory after a successful pull. Default is ``True``.
    csv_prefix : str, optional
        A string prefix prepended to the generated CSV filename. Default is
        an empty string.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 create_csv: bool = True, csv_prefix: str = "", sub_directory: str = ""):
        super().__init__(config, start_time, end_time, create_csv, csv_prefix, sub_directory)

    def raw_data_to_df(self, config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
        """Fetch sensor data from the BlueBot Flow API and return it as a DataFrame.

        Parameters
        ----------
        config : ConfigManager
            The ConfigManager object used to retrieve ``config.api_token``
            (full ``PREFIX.VALUE`` key) for the authorization header.
        startTime : datetime, optional
            Start of the query window.
        endTime : datetime, optional
            End of the query window. Defaults to ``datetime.now()`` if not provided.

        Returns
        -------
        pd.DataFrame
            A DataFrame indexed by timestamp with one column per sensor.
            Returns an empty DataFrame if the request fails or an error occurs.
        """
        if endTime is None:
            endTime = datetime.now()
        if startTime is None:
            startTime = endTime - timedelta(hours=28)

        # The API key is stored as PREFIX.VALUE; only the VALUE portion is used.
        api_key = config.api_token.split('.', 1)[-1]

        url = f'{BASE_URL}/total/one-minute/{config.api_device_id}'
        params = {
            'range_start': int(startTime.timestamp()),
            'range_end': int(endTime.timestamp()),
        }
        headers = {
            'Content-Type': 'application/json',
            'bluebot-api-key': f'{api_key}',
        }

        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                response_json = response.json()
                df = pd.DataFrame(response_json)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_localize(None)
                df = df.pivot(index='timestamp', columns='device_id', values='total')
                df.columns = ['total_flow']
                df.columns.name = None
                df = df.sort_index()
                return df
            else:
                print(f"Failed to make GET request. Status code: {response.status_code} {response.json()}")
                return pd.DataFrame()
        except Exception as e:
            traceback.print_exc()
            print(f"An error occurred: {e}")
            return pd.DataFrame()
