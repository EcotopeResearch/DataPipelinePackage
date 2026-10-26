import io
import pandas as pd
import requests
from ecopipeline import ConfigManager
from ecopipeline.extract.APIExtractor import APIExtractor
from datetime import datetime, timedelta


class PiExtractor(APIExtractor):
    """APIExtractor for the data interface on an Ecotope Pi monitoring unit.

    Pulls the unit's CSV log rows from ``/api/v1/data`` between ``start_time``
    and ``end_time``. The unit allows at most 31 days per request, so longer
    windows are pulled one 31-day block at a time.

    The unit writes timestamps in the Pi's local system time with no time zone,
    and refuses request times that carry one, so ``start_time`` and
    ``end_time`` must be naive local times and the returned index is naive.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the
        pipeline, including the unit's data token (``config.api_token``),
        address (``config.api_host``) and port (``config.api_port``).
    start_time : datetime, optional
        The start of the data extraction window (included). Defaults to one
        day before ``end_time`` if not provided.
    end_time : datetime, optional
        The end of the data extraction window (not included). Defaults to
        ``datetime.now()`` if not provided.
    create_csv : bool, optional
        If ``True``, writes the raw DataFrame to a CSV file in the configured
        data directory after a successful pull. Default is ``True``.
    csv_prefix : str, optional
        A string prefix prepended to the generated CSV filename. Default is
        an empty string.
    timeout : float, optional
        Seconds to wait for each request. Default is 60.
    """

    MAX_RANGE_DAYS = 31
    
    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 create_csv: bool = True, csv_prefix: str = "", sub_directory: str = "", timeout: float = 60):
        self.timeout = timeout
        super().__init__(config, start_time, end_time, create_csv, csv_prefix, sub_directory)

    def raw_data_to_df(self, config: ConfigManager, startTime: datetime = None, endTime: datetime = None) -> pd.DataFrame:
        """Fetch log rows from the Pi data interface and return them as a DataFrame.

        Parameters
        ----------
        config : ConfigManager
            The ConfigManager object used to retrieve ``config.api_token`` for
            the Bearer authorisation header and ``config.api_host`` /
            ``config.api_port`` for the unit's address.
        startTime : datetime, optional
            Start of the query window (included). Defaults to one day before
            ``endTime`` if not provided.
        endTime : datetime, optional
            End of the query window (not included). Defaults to
            ``datetime.now()`` if not provided.

        Returns
        -------
        pd.DataFrame
            A DataFrame indexed by ``time_pt`` (naive Pi local time) with one
            column per logged point. Blank cells are ``NaN``. Returns an empty
            DataFrame if no rows exist for the requested time frame.
        """
        if config.api_token is None or config.api_host is None:
            raise Exception("api_token and api_host must be set in the [data] section of config.ini to pull from a Pi unit.")
        if endTime is None:
            endTime = datetime.now()
        if startTime is None:
            startTime = endTime - timedelta(days=1)

        base_url = f"http://{config.api_host}:{config.api_port}/api/v1"
        headers = {'Authorization': f'Bearer {config.api_token}'}

        temp_dfs = []
        time_parser = startTime
        while time_parser < endTime:
            time_parse_end = min(time_parser + timedelta(days=self.MAX_RANGE_DAYS), endTime)
            params = {
                'start': time_parser.strftime('%Y-%m-%dT%H:%M:%S'),
                'end': time_parse_end.strftime('%Y-%m-%dT%H:%M:%S'),
            }
            response = requests.get(f"{base_url}/data", headers=headers, params=params, timeout=self.timeout)
            if response.status_code != 200:
                try:
                    error = response.json().get('error', response.text)
                except ValueError:
                    error = response.text
                raise Exception(f"Pi data request for {params['start']} to {params['end']} failed. "
                                f"Status code: {response.status_code} {error}")
            df = pd.read_csv(io.StringIO(response.text))
            if not df.empty:
                temp_dfs.append(df)
            time_parser = time_parse_end

        if len(temp_dfs) == 0:
            print("No Pi data retrieved for time frame.")
            return pd.DataFrame()
        df = pd.concat(temp_dfs, ignore_index=True)
        df['time_pt'] = pd.to_datetime(df.pop('timestamp'), format='ISO8601')
        df = df.set_index('time_pt').sort_index()
        return df
