import requests
import pandas as pd
# from datetime import datetime, timedelta
# import os
# import gzip
# import urllib.request
from io import StringIO

class NOAADataDownloader:
    def __init__(self, station_code, api_token=None):
        """
        Initialize downloader for a specific weather station
        
        Args:
            station_code (str): Airport code (e.g., 'KLAX', 'LAX', 'JFK', 'ORD')
            api_token (str, optional): NOAA API token for daily data access
        """
        self.station_code = station_code.upper().strip()
        self.api_token = api_token
        self.base_url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/"
        
        # Clean airport code - add K if not present for US airports
        if len(self.station_code) == 3 and not self.station_code.startswith('K'):
            self.station_code = 'K' + self.station_code
        
        # Find station information
        self.station_info = self._find_station_info()
        
        if not self.station_info:
            raise ValueError(f"Could not find weather station for {station_code}")
        
        print(f"Initialized downloader for: {self.station_info['name']}")
        if self.station_info.get('usaf') and self.station_info.get('wban'):
            print(f"ISD Station ID: {self.station_info['usaf']}-{self.station_info['wban']}")
        if self.station_info.get('ghcn_id'):
            print(f"GHCN-D Station ID: {self.station_info['ghcn_id']}")
    
    def _find_station_info(self):
        """Find station information for the given airport code"""
        
        # First try common stations mapping
        common_stations = self._get_common_stations()
        if self.station_code in common_stations:
            return common_stations[self.station_code]
        
        # Try searching ISD station history
        isd_station = self._search_isd_stations()
        if isd_station:
            return isd_station
        
        # Try API search if token available
        if self.api_token:
            api_station = self._search_api_stations()
            if api_station:
                return api_station
        
        return None
    
    def _get_common_stations(self):
        """Return mapping of common airport codes to station information"""
        return {
            'KLAX': {
                'name': 'LOS ANGELES INTERNATIONAL AIRPORT',
                'usaf': '722950',
                'wban': '23174',
                'ghcn_id': 'GHCND:USW00023174',
                'latitude': 33.938,
                'longitude': -118.389,
                'elevation': 32.0
            },
            'KJFK': {
                'name': 'JOHN F KENNEDY INTERNATIONAL AIRPORT',
                'usaf': '744860',
                'wban': '94789',
                'ghcn_id': 'GHCND:USW00094789',
                'latitude': 40.640,
                'longitude': -73.779,
                'elevation': 3.4
            },
            'KORD': {
                'name': 'CHICAGO OHARE INTERNATIONAL AIRPORT',
                'usaf': '725300',
                'wban': '94846',
                'ghcn_id': 'GHCND:USW00094846',
                'latitude': 41.995,
                'longitude': -87.934,
                'elevation': 201.5
            },
            'KDEN': {
                'name': 'DENVER INTERNATIONAL AIRPORT',
                'usaf': '725650',
                'wban': '03017',
                'ghcn_id': 'GHCND:USW00003017',
                'latitude': 39.833,
                'longitude': -104.65,
                'elevation': 1640.0
            },
            'KATL': {
                'name': 'HARTSFIELD JACKSON ATLANTA INTERNATIONAL AIRPORT',
                'usaf': '722190',
                'wban': '13874',
                'ghcn_id': 'GHCND:USW00013874',
                'latitude': 33.640,
                'longitude': -84.427,
                'elevation': 308.5
            },
            'KMIA': {
                'name': 'MIAMI INTERNATIONAL AIRPORT',
                'usaf': '722020',
                'wban': '12839',
                'ghcn_id': 'GHCND:USW00012839',
                'latitude': 25.793,
                'longitude': -80.290,
                'elevation': 11.0
            },
            'KSEA': {
                'name': 'SEATTLE TACOMA INTERNATIONAL AIRPORT',
                'usaf': '727930',
                'wban': '24233',
                'ghcn_id': 'GHCND:USW00024233',
                'latitude': 47.449,
                'longitude': -122.309,
                'elevation': 131.1
            },
            'KBOS': {
                'name': 'BOSTON LOGAN INTERNATIONAL AIRPORT',
                'usaf': '725090',
                'wban': '14739',
                'ghcn_id': 'GHCND:USW00014739',
                'latitude': 42.361,
                'longitude': -71.020,
                'elevation': 6.1
            },
            'KPHX': {
                'name': 'PHOENIX SKY HARBOR INTERNATIONAL AIRPORT',
                'usaf': '722780',
                'wban': '23183',
                'ghcn_id': 'GHCND:USW00023183',
                'latitude': 33.434,
                'longitude': -112.008,
                'elevation': 337.1
            },
            'KLAS': {
                'name': 'LAS VEGAS MCCARRAN INTERNATIONAL AIRPORT',
                'usaf': '723860',
                'wban': '23169',
                'ghcn_id': 'GHCND:USW00023169',
                'latitude': 36.080,
                'longitude': -115.152,
                'elevation': 664.1
            }
        }
    
    def _search_isd_stations(self):
        """Search ISD station history for the airport"""
        try:
            url = "https://www.ncei.noaa.gov/data/global-hourly/doc/isd-history.csv"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            df = pd.read_csv(StringIO(response.text))
            
            # Search for airport code in station name
            search_terms = [
                self.station_code.replace('K', ''),  # LAX from KLAX
                self.station_code,                   # KLAX
                self.station_code + ' ',             # Exact match with space
            ]
            
            for term in search_terms:
                mask = df['STATION NAME'].str.contains(term, case=False, na=False)
                matches = df[mask]
                
                if not matches.empty:
                    # Take the first match with recent data
                    best_match = matches.iloc[0]
                    
                    return {
                        'name': best_match['STATION NAME'],
                        'usaf': str(best_match['USAF']).zfill(6),
                        'wban': str(best_match['WBAN']).zfill(5),
                        'country': best_match['CTRY'],
                        'state': best_match.get('STATE', ''),
                        'latitude': best_match['LAT'],
                        'longitude': best_match['LON'],
                        'elevation': best_match['ELEV(M)'],
                        'begin_date': str(best_match['BEGIN']),
                        'end_date': str(best_match['END'])
                    }
            
            return None
            
        except Exception as e:
            print(f"ISD search failed: {e}")
            return None
    
    def _search_api_stations(self):
        """Search for stations using NOAA API"""
        if not self.api_token:
            return None
            
        try:
            url = f"{self.base_url}stations"
            params = {'limit': 100, 'format': 'json'}
            headers = {"token": self.api_token}
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if 'results' in data:
                search_terms = [self.station_code.replace('K', ''), self.station_code]
                
                for station in data['results']:
                    name = station.get('name', '').upper()
                    for term in search_terms:
                        if term in name:
                            return {
                                'name': station.get('name'),
                                'ghcn_id': station.get('id'),
                                'latitude': station.get('latitude'),
                                'longitude': station.get('longitude'),
                                'elevation': station.get('elevation'),
                                'mindate': station.get('mindate'),
                                'maxdate': station.get('maxdate')
                            }
            
            return None
            
        except Exception as e:
            print(f"API search failed: {e}")
            return None
    
    def get_station_info(self):
        """Return station information"""
        return self.station_info.copy()
    
    # def download_hourly_data(self, start_date, end_date, data_types=None):
    #     """
    #     Download hourly weather data using NOAA's data access API
        
    #     Args:
    #         start_date (str or pd.Timestamp): Start date in YYYY-MM-DD format or pandas Timestamp or datetime
    #         end_date (str or pd.Timestamp): End date in YYYY-MM-DD format or pandas Timestamp or datetime
    #         data_types (list, optional): List of data types to download
            
    #     Returns:
    #         pandas.DataFrame: Hourly weather data
    #     """
    #     if not (self.station_info.get('usaf') and self.station_info.get('wban')):
    #         raise ValueError("Station does not have ISD identifiers for hourly data")
        
    #     # Convert pd.Timestamp to string format if needed
    #     if isinstance(start_date, pd.Timestamp):
    #         start_date = start_date.strftime('%Y-%m-%d')
    #     elif hasattr(start_date, 'strftime'):  # datetime.datetime or similar
    #         start_date = start_date.strftime('%Y-%m-%d')
            
    #     if isinstance(end_date, pd.Timestamp):
    #         end_date = end_date.strftime('%Y-%m-%d')
    #     elif hasattr(end_date, 'strftime'):  # datetime.datetime or similar
    #         end_date = end_date.strftime('%Y-%m-%d')
        
    #     # Create station ID in format expected by the API
    #     station_id = f"{self.station_info['usaf']}{self.station_info['wban']}"
    #     # station_id = "USW00023174"#"USC00457180"
    #     # print("station_id is ",station_id)
    #     # Default data types for hourly weather data
    #     if not data_types:
    #         data_types = [
    #             'TMP',      # Temperature
    #             'DEW',      # Dew point
    #             'SLP',      # Sea level pressure  
    #             'WND',      # Wind direction and speed
    #             'VIS',      # Visibility
    #             'AA1'       # Precipitation (if available)
    #         ]
        
    #     # NOAA's data access API endpoint
    #     base_url = "https://www.ncei.noaa.gov/access/services/data/v1"
        
    #     params = {
    #         'dataset': 'global-hourly',
    #         # 'dataTypes': 'TMP',#','.join(data_types),
    #         'stations': station_id,
    #         'startDate': start_date,
    #         'endDate': end_date,
    #         'format': 'json',
    #         'includeAttributes': 'true',
    #         'includeStationName': 'true',
    #         'includeStationLocation': 'true'
    #     }
        
    #     try:
    #         print(f"Downloading hourly data from {start_date} to {end_date}...")
    #         print(f"Station: {station_id} ({self.station_info.get('name', 'Unknown')})")
    #         full_url = requests.Request('GET', base_url, params=params).prepare().url
    #         print(f"API Request URL:")
    #         print(f"{full_url}")
    #         print()
    #         # https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-hourly
    #         # &dataTypes=TMP%2CDEW%2CSLP%2CWND%2CVIS%2CAA1&stations=USW00023174&startDate=2025-08-26&endDate=2025-09-18&format=json
    #         # &includeAttributes=true&includeStationName=true&includeStationLocation=true

    #         # https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-summary-of-the-year
    #         # &dataTypes=DP01,DP05,DP10,DSND,DSNW,DT00,DT32,DX32,DX70,DX90,SNOW,PRCP&stations=ASN00084027&startDate=1952-01-01&endDate=1970-12-31&includeAttributes=true&format=pdf
            
    #         response = requests.get(base_url, params=params, timeout=60)
    #         response.raise_for_status()
            
    #         # Parse JSON response
    #         data = response.json()
            
    #         if not data:
    #             print("No data returned from API")
    #             return pd.DataFrame()
            
    #         # Convert to DataFrame
    #         df = pd.DataFrame(data)
            
    #         if df.empty:
    #             print("No hourly data found for the specified parameters")
    #             return pd.DataFrame()
            
    #         # Process the data
    #         df = self._process_hourly_data(df)
            
    #         print(f"Successfully downloaded {len(df)} hourly records")
    #         return df
            
    #     except requests.exceptions.RequestException as e:
    #         print(f"API request failed: {e}")
    #         if hasattr(e, 'response') and e.response is not None:
    #             print(f"Response status: {e.response.status_code}")
    #             print(f"Response text: {e.response.text[:500]}...")
    #         return pd.DataFrame()
    #     except Exception as e:
    #         print(f"Failed to download hourly data: {e}")
    #         return pd.DataFrame()
    
    # def _process_hourly_data(self, df):
    #     """Process and clean hourly data from NOAA API"""
    #     try:
    #         # Convert DATE to datetime
    #         if 'DATE' in df.columns:
    #             df['datetime'] = pd.to_datetime(df['DATE'], errors='coerce')
    #             df = df.dropna(subset=['datetime'])
    #             df = df.sort_values('datetime')
            
    #         # Process temperature data (convert tenths of degrees C to C)
    #         if 'TMP' in df.columns:
    #             df['temperature_c'] = pd.to_numeric(df['TMP'], errors='coerce') / 10
    #             df['temperature_f'] = df['temperature_c'] * 9/5 + 32
            
    #         # Process dew point data
    #         if 'DEW' in df.columns:
    #             df['dewpoint_c'] = pd.to_numeric(df['DEW'], errors='coerce') / 10
    #             df['dewpoint_f'] = df['dewpoint_c'] * 9/5 + 32
            
    #         # Process sea level pressure (convert tenths of hPa to hPa)
    #         if 'SLP' in df.columns:
    #             df['pressure_hpa'] = pd.to_numeric(df['SLP'], errors='coerce') / 10
            
    #         # Process wind data - format is typically "999,9" (direction,speed)
    #         if 'WND' in df.columns:
    #             wind_data = df['WND'].astype(str)
                
    #             # Extract wind direction and speed
    #             wind_direction = []
    #             wind_speed = []
                
    #             for wind_str in wind_data:
    #                 try:
    #                     if ',' in wind_str:
    #                         dir_str, speed_str = wind_str.split(',')[:2]
                            
    #                         # Wind direction (degrees)
    #                         direction = int(dir_str) if dir_str != '999' else None
    #                         wind_direction.append(direction)
                            
    #                         # Wind speed (tenths of m/s to m/s)
    #                         speed = float(speed_str) / 10 if speed_str != '9999' else None
    #                         wind_speed.append(speed)
    #                     else:
    #                         wind_direction.append(None)
    #                         wind_speed.append(None)
    #                 except (ValueError, IndexError):
    #                     wind_direction.append(None)
    #                     wind_speed.append(None)
                
    #             df['wind_direction'] = wind_direction
    #             df['wind_speed_mps'] = wind_speed
    #             df['wind_speed_kmh'] = pd.Series(wind_speed) * 3.6
    #             df['wind_speed_mph'] = pd.Series(wind_speed) * 2.237
            
    #         # Process visibility (meters)
    #         if 'VIS' in df.columns:
    #             df['visibility_m'] = pd.to_numeric(df['VIS'], errors='coerce')
    #             df['visibility_km'] = df['visibility_m'] / 1000
    #             df['visibility_mi'] = df['visibility_m'] / 1609.34
            
    #         # Add station information columns
    #         if 'STATION' in df.columns:
    #             df['station_id'] = df['STATION']
            
    #         if 'NAME' in df.columns:
    #             df['station_name'] = df['NAME']
            
    #         if 'LATITUDE' in df.columns:
    #             df['latitude'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
            
    #         if 'LONGITUDE' in df.columns:
    #             df['longitude'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
            
    #         if 'ELEVATION' in df.columns:
    #             df['elevation_m'] = pd.to_numeric(df['ELEVATION'], errors='coerce')
            
    #         return df
            
    #     except Exception as e:
    #         print(f"Error processing hourly data: {e}")
    #         return df
        

    def download_daily_TAVG_data(self, start_date, end_date, convert_to_fahrenheit = True):
        """
        Download daily Average Temperature data using NOAA API
        
        Args:
            start_date (str or pd.Timestamp): Start date in YYYY-MM-DD format or pandas Timestamp or datetime
            end_date (str or pd.Timestamp): End date in YYYY-MM-DD format or pandas Timestamp or datetime
            convert_to_fahrenheit (bool): converts temperature values to fahrenhiet. Otherwise will be celcius*10 
            
        Returns:
            pandas.DataFrame: Daily weather data
        """
        if not self.api_token:
            raise ValueError("API token required for daily data. Get one from https://www.ncdc.noaa.gov/cdo-web/token")
        
        if not self.station_info.get('ghcn_id'):
            raise ValueError("Station does not have GHCN-D identifier for daily data")
        
        # Convert pd.Timestamp to string format if needed
        if isinstance(start_date, pd.Timestamp):
            start_date = start_date.strftime('%Y-%m-%d')
        elif hasattr(start_date, 'strftime'):  # datetime.datetime or similar
            start_date = start_date.strftime('%Y-%m-%d')
            
        if isinstance(end_date, pd.Timestamp):
            end_date = end_date.strftime('%Y-%m-%d')
        elif hasattr(end_date, 'strftime'):  # datetime.datetime or similar
            end_date = end_date.strftime('%Y-%m-%d')
        
        # if not datatypes:
        #     datatypes = ['TAVG']
        
        url = f"{self.base_url}data"
        params = {
            'datasetid': 'GHCND',
            'stationid': self.station_info['ghcn_id'],
            'startdate': start_date,
            'enddate': end_date,
            'datatypeid': 'TAVG',
            'limit': 1000,
            'format': 'json'
        }
        
        try:
            print(f"Downloading daily data from {start_date} to {end_date}...")
            
            headers = {"token": self.api_token}
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            if 'results' in data:
                df = pd.DataFrame(data['results'])
                
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    # Convert value from tenths of Celsius to Fahrenheit
                    df['value'] = (df['value'] / 10) * 9/5 + 32
                    df = df.set_index('date')
                    df = df[['value']].rename(columns={'value': 'OAT_NOAA'})
                    
                    print(f"Successfully downloaded {len(df)} daily records")
                    return df
                else:
                    print("No daily data found for the specified parameters")
                    return pd.DataFrame()
            else:
                print("No daily data found")
                return pd.DataFrame()
                
        except requests.exceptions.RequestException as e:
            print(f"Daily data download failed: {e}")
            return pd.DataFrame()