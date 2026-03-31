import pandas as pd
import requests
import traceback
from ecopipeline import ConfigManager
from ecopipeline.extract.APIExtractor import APIExtractor
from datetime import datetime, timedelta


class LiCOR(APIExtractor):
    """APIExtractor for the LI-COR Cloud API.

    Queries sensor data for the configured device between ``start_time`` and
    ``end_time``.  Returns a DataFrame indexed by UTC timestamp with sensor
    serial numbers as columns.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the
        pipeline, including the LI-COR API token (``config.api_token``) and
        the device serial number (``config.api_device_id``).
    start_time : datetime, optional
        The start of the data extraction window. Defaults to 28 hours before
        ``end_time`` if not provided.
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
                 create_csv: bool = True, csv_prefix: str = ""):
        super().__init__(config, start_time, end_time, create_csv, csv_prefix)

    def raw_data_to_df(self, config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
        """Fetch sensor data from the LI-COR Cloud API and return it as a DataFrame.

        Calls the ``/v2/data`` endpoint, iterates over each sensor returned in
        the response, and assembles a wide-format DataFrame keyed by
        millisecond-precision UTC timestamps.

        Parameters
        ----------
        config : ConfigManager
            The ConfigManager object used to retrieve ``config.api_token`` for
            the Bearer authorisation header and ``config.api_device_id`` for
            the device serial number query parameter.
        startTime : datetime, optional
            Start of the query window. Defaults to 28 hours before
            ``endTime`` if not provided.
        endTime : datetime, optional
            End of the query window. Defaults to ``datetime.now()`` if not
            provided.

        Returns
        -------
        pd.DataFrame
            A DataFrame indexed by UTC ``datetime`` (millisecond precision)
            with one column per sensor serial number.  Non-numeric values are
            coerced to ``None`` via :py:meth:`_get_float_value`.  Returns an
            empty DataFrame if the request fails or an error occurs.
        """
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
