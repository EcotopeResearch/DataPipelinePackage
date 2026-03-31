import pandas as pd
import requests
from ecopipeline import ConfigManager
from ecopipeline.extract.APIExtractor import APIExtractor
from datetime import datetime, timedelta


class FieldManager(APIExtractor):
    """APIExtractor for the FieldPop / Field Manager API.

    Recursively splits the time range in half whenever the server returns a 500
    'log size too large' response, down to a minimum of 30-minute windows.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline,
        including the FieldPop API token and device ID.
    start_time : datetime, optional
        The start of the data extraction window. Defaults to
        ``datetime(2000, 1, 1, 0, 0, 0)`` if not provided.
    end_time : datetime, optional
        The end of the data extraction window. Defaults to
        ``datetime.now()`` if not provided.
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
        """Fetch sensor data from the FieldPop API and return it as a DataFrame.

        Queries the ``fieldpop-api/deviceDataLog`` endpoint for the given time
        range.  If the server responds with a 500 error indicating that the log
        size is too large, the method recursively bisects the time window until
        each sub-window is no smaller than 30 minutes.

        Parameters
        ----------
        config : ConfigManager
            The ConfigManager object used to retrieve the FieldPop API token
            (``get_fm_token``) and device ID (``get_fm_device_id``).
        startTime : datetime, optional
            Start of the query window. Defaults to
            ``datetime(2000, 1, 1, 0, 0, 0)`` if not provided.
        endTime : datetime, optional
            End of the query window. Defaults to ``datetime.now()`` if not
            provided.

        Returns
        -------
        pd.DataFrame
            A DataFrame indexed by ``time_pt`` (UTC timestamps converted to
            ``datetime``) with one column per sensor, values aggregated by
            mean when multiple readings share the same timestamp.  Returns an
            empty DataFrame if the request fails or an error occurs.
        """
        if startTime is None:
            startTime = datetime(2000, 1, 1, 0, 0, 0)
        if endTime is None:
            endTime = datetime.now()

        api_token = config.get_fm_token()
        device_id = config.get_fm_device_id()
        url = (f"https://www.fieldpop.io/rest/method/fieldpop-api/deviceDataLog"
               f"?happn_token={api_token}&deviceID={device_id}"
               f"&startUTCsec={int(startTime.timestamp())}&endUTCsec={int(endTime.timestamp())}")

        try:
            response = requests.get(url)
            if response.status_code == 200:
                df = pd.DataFrame()
                data = response.json()['data']
                for key, value in data.items():
                    for sensor, records in value.items():
                        sensor_rows = [{'time_pt': entry['time'], sensor: entry['value']} for entry in records]
                        df = pd.concat([df, pd.DataFrame(sensor_rows)])
                if not df.empty:
                    df['time_pt'] = pd.to_datetime(df['time_pt'], unit='s')
                    df.set_index('time_pt', inplace=True)
                    df = df.sort_index()
                    df = df.groupby(df.index).mean()
                return df

            if response.status_code == 500:
                json_message = response.json()
                too_large = 'The log size is too large - please try again with a smaller date range.'
                if ('error' in json_message and 'message' in json_message['error']
                        and json_message['error']['message'] == too_large):
                    if endTime - timedelta(minutes=30) < startTime:
                        print(f"Unable to retrieve data for {startTime} - {endTime}")
                        return pd.DataFrame()
                    mid = startTime + (endTime - startTime) / 2
                    df_1 = self.raw_data_to_df(config, startTime, mid)
                    df_2 = self.raw_data_to_df(config, mid, endTime)
                    df = pd.concat([df_1, df_2])
                    df = df.sort_index()
                    df = df.groupby(df.index).mean()
                    return df

            print(f"Failed to make GET request. Status code: {response.status_code}")
            return pd.DataFrame()
        except Exception as e:
            print(f"An error occurred: {e}")
            return pd.DataFrame()
