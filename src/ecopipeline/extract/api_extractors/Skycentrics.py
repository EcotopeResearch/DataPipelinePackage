import pandas as pd
import gzip
import json
import requests
from ecopipeline import ConfigManager
from ecopipeline.extract.APIExtractor import APIExtractor
from datetime import datetime, timedelta


class Skycentrics(APIExtractor):
    """APIExtractor for the Skycentrics API.

    Pulls data day-by-day between ``start_time`` and ``end_time``, normalises
    the JSON sensor records into a pivot table, and rounds 59:59 timestamps up
    to the next minute.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the
        pipeline, including the Skycentrics API token
        (``config.get_skycentrics_token``) and device ID
        (``config.api_device_id``).
    start_time : datetime, optional
        The start of the data extraction window. Defaults to one day before
        ``end_time`` if not provided.
    end_time : datetime, optional
        The end of the data extraction window. Defaults to
        ``datetime.utcnow()`` if not provided.
    create_csv : bool, optional
        If ``True``, writes the raw DataFrame to a CSV file in the configured
        data directory after a successful pull. Default is ``True``.
    csv_prefix : str, optional
        A string prefix prepended to the generated CSV filename. Default is
        an empty string.
    time_zone : str, optional
        The timezone string used to localise timestamps after converting from
        UTC. Default is ``'US/Pacific'``.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 create_csv: bool = True, csv_prefix: str = "", time_zone: str = 'US/Pacific'):
        self.time_zone = time_zone
        super().__init__(config, start_time, end_time, create_csv, csv_prefix)

    def raw_data_to_df(self, config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
        """Fetch sensor data from the Skycentrics API and return it as a DataFrame.

        Iterates day-by-day from ``startTime`` to ``endTime``, issuing one
        authenticated GET request per day.  Each gzip-compressed JSON response
        is decompressed, normalised with :py:func:`pandas.json_normalize`, and
        pivoted so that sensor IDs become columns.  Timestamps that fall at
        second 59:59 of a minute are nudged forward by one second to align with
        the top of the next minute.

        Parameters
        ----------
        config : ConfigManager
            The ConfigManager object used to retrieve the per-request
            Skycentrics HMAC token via ``config.get_skycentrics_token`` and
            the device ID via ``config.api_device_id``.
        startTime : datetime, optional
            Start of the query window (UTC). Defaults to one day before
            ``endTime`` if not provided.
        endTime : datetime, optional
            End of the query window (UTC). Defaults to ``datetime.utcnow()``
            if not provided.

        Returns
        -------
        pd.DataFrame
            A DataFrame indexed by ``time_pt`` (timezone-aware timestamps
            converted to ``self.time_zone``) with one column per sensor ID.
            Returns an empty DataFrame if no data is retrieved for the
            requested time frame.
        """
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
