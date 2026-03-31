import configparser
import os
import mysql.connector
import mysql.connector.cursor
import requests
from datetime import datetime
import base64
import hashlib
import hmac
import pandas as pd

class ConfigManager:
    """Manage configuration, directory paths, database credentials, and API tokens for a data pipeline.

    Reads a ``config.ini`` file to populate connection and directory settings.
    Optionally remaps Windows-style drive letters to POSIX paths when running on
    Ecotope's server infrastructure.

    Parameters
    ----------
    config_file_path : str, optional
        Path to the ``config.ini`` file for the pipeline
        (e.g. ``"full/path/to/config.ini"``). Must contain login information
        for the MySQL database where data is to be loaded. Defaults to
        ``"config.ini"``.
    input_directory : str, optional
        Path to the input directory for the pipeline
        (e.g. ``"full/path/to/pipeline/input/"``). Defaults to the value
        defined in the ``[input]`` section of the config file.
    output_directory : str, optional
        Path to the output directory for the pipeline
        (e.g. ``"full/path/to/pipeline/output/"``). Defaults to the value
        defined in the ``[output]`` section of the config file.
    data_directory : str, optional
        Path to the data directory for the pipeline
        (e.g. ``"full/path/to/pipeline/data/"``). Defaults to the value
        defined in the ``[data]`` section of the config file.
    eco_file_structure : bool, optional
        Set to ``True`` when the pipeline runs on Ecotope's server so that
        Windows drive-letter prefixes (``R:``, ``F:``) are remapped to the
        correct POSIX mount points. Defaults to ``False``.

    Attributes
    ----------
    config_directory : str
        Resolved path to the ``config.ini`` file.
    input_directory : str
        Resolved path to the pipeline input directory.
    output_directory : str
        Resolved path to the pipeline output directory.
    data_directory : str
        Resolved path to the pipeline data directory.
    api_usr : str or None
        API username read from the config file, if present.
    api_pw : str or None
        API password read from the config file, if present.
    api_token : str or None
        API token read from the config file, if present.
    api_secret : str or None
        API secret read from the config file, if present.
    api_device_id : str or None
        API device ID read from the config file, if present.
    db_connection_info : dict
        Dictionary containing ``user``, ``password``, ``host``, and
        ``database`` keys used to open MySQL connections.

    Raises
    ------
    Exception
        If ``config_file_path`` does not exist on the filesystem.
    Exception
        If the ``[input]`` section or its ``directory`` key is missing from
        the config file.
    Exception
        If the ``[output]`` section or its ``directory`` key is missing from
        the config file.
    Exception
        If the ``[data]`` section is missing or contains no recognised data
        source configuration.
    Exception
        If any of the resolved directory paths do not exist on the filesystem.
    """
    def __init__(self, config_file_path : str = "config.ini", input_directory : str = None, output_directory : str = None, data_directory : str = None, eco_file_structure : bool = False):
        print(f"<<<==================== CONFIGMANAGER INITIALIZED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ====================>>>")
        
        os.chdir(os.getcwd())
        
        self.config_directory = config_file_path

        if not os.path.exists(self.config_directory):
            raise Exception(f"File path '{self.config_directory}' does not exist.")

        configure = configparser.ConfigParser()
        configure.read(self.config_directory)

        # Directories are saved in config.ini with a relative directory to working directory
        self.input_directory = input_directory
        if self.input_directory is None:
            if 'input' in configure and 'directory' in configure['input']:
                self.input_directory = configure.get('input', 'directory')
            else:
                raise Exception('input section missing or incomplete in configuration file.')
        self.output_directory = output_directory
        if self.output_directory is None:
            if 'input' in configure and 'directory' in configure['output']:
                self.output_directory = configure.get('output', 'directory')
            else:
                raise Exception('output section missing or incomplete in configuration file.')
        self.data_directory = data_directory
        self.api_usr = None
        self.api_pw = None
        self.api_token = None
        self.api_secret = None
        self.api_device_id = None
        if self.data_directory is None:
            configured_data_method = False
            if 'data' in configure:
                if 'directory' in configure['data']:
                    self.data_directory = configure.get('data', 'directory')
                    configured_data_method = True
                if 'fieldManager_api_usr' in configure['data'] and 'fieldManager_api_pw' in configure['data'] and 'fieldManager_device_id' in configure['data']:
                    # LEGACY, Remove when you can
                    self.api_usr = configure.get('data', 'fieldManager_api_usr')
                    self.api_pw = configure.get('data', 'fieldManager_api_pw')
                    self.api_device_id = configure.get('data','fieldManager_device_id')
                    configured_data_method = True
                elif 'api_usr' in configure['data'] and 'api_pw' in configure['data'] and 'device_id' in configure['data']:
                    self.api_usr = configure.get('data', 'api_usr')
                    self.api_pw = configure.get('data', 'api_pw')
                    self.api_device_id = configure.get('data','device_id')
                    configured_data_method = True
                elif 'api_token' in configure['data']:
                    self.api_token = configure.get('data', 'api_token')
                    if 'api_secret' in configure['data']:
                        self.api_secret = configure.get('data', 'api_secret')
                    self.api_device_id = configure.get('data','device_id')
                    configured_data_method = True
            if not configured_data_method:
                raise Exception('data configuration section missing or incomplete in configuration file.')

        # If working on compute3, change directory (Ecotope specific)
        if eco_file_structure and os.name == 'posix':
            if self.input_directory[:2] == 'R:':
                self.input_directory = '/storage/RBSA_secure' + self.input_directory[2:]
                self.output_directory = '/storage/RBSA_secure' + self.output_directory[2:]
                self.data_directory = '/storage/RBSA_secure' + self.data_directory[2:]
            elif self.input_directory[:2] == 'F:':
                self.input_directory = '/storage/CONSULT' + self.input_directory[2:]
                self.output_directory = '/storage/CONSULT' + self.output_directory[2:]
                self.data_directory = '/storage/CONSULT' + self.data_directory[2:]

        directories = [self.input_directory, self.output_directory, self.data_directory]
        for directory in directories:
            if not os.path.isdir(directory):
                raise Exception(f"File path '{directory}' does not exist, check directories in config.ini.")
            
        self.db_connection_info = {
                'user': configure.get('database', 'user'),
                'password': configure.get('database', 'password'),
                'host': configure.get('database', 'host'),
                'database': configure.get('database', 'database')
            }
    
    def get_var_names_path(self) -> str:
        """Return the full path to the ``Variable_Names.csv`` file.

        The file is expected to reside directly inside the pipeline's input
        directory (e.g. ``"full/path/to/pipeline/input/Variable_Names.csv"``).

        Returns
        -------
        str
            Absolute path to ``Variable_Names.csv``.
        """
        return f"{self.input_directory}Variable_Names.csv"

    def get_event_log_path(self) -> str:
        """Return the full path to the ``Event_Log.csv`` file.

        The file is expected to reside directly inside the pipeline's input
        directory (e.g. ``"full/path/to/pipeline/input/Event_Log.csv"``).

        Returns
        -------
        str
            Absolute path to ``Event_Log.csv``.
        """
        return f"{self.input_directory}Event_Log.csv"

    def get_weather_dir_path(self) -> str:
        """Return the path to the directory that holds NOAA weather data files.

        The directory is expected to reside directly inside the pipeline's data
        directory (e.g. ``"full/path/to/pipeline/data/weather"``).

        Returns
        -------
        str
            Path to the ``weather`` subdirectory within the data directory.
        """
        return f"{self.data_directory}weather"
    
    def get_db_table_info(self, table_headers : list) -> dict:
        """Read table configuration from the config file and return a combined info dict.

        For each header in ``table_headers``, the corresponding ``table_name``
        value is read from the matching section of ``config.ini``. The name of
        the configured database is also included in the result under the key
        ``"database"``.

        Parameters
        ----------
        table_headers : list
            Section headers from ``config.ini`` whose ``table_name`` values
            should be retrieved. Each entry must exactly match a section name
            in the config file.

        Returns
        -------
        dict
            A dictionary mapping each header to a nested dict with a
            ``"table_name"`` key, plus a top-level ``"database"`` key
            containing the database name from the stored connection info.
        """

        db_table_info = {}
        if len(table_headers) > 0:
            configure = configparser.ConfigParser()
            configure.read(self.config_directory)
            db_table_info = {header: {"table_name": configure.get(header, 'table_name')} for header in table_headers}
        db_table_info["database"] = self.db_connection_info["database"]

        print(f"Successfully fetched configuration information from file path {self.config_directory}.")
        return db_table_info
    
    def get_table_name(self, header: str) -> str:
        """Return the ``table_name`` value for the given config file section.

        Parameters
        ----------
        header : str
            Section header in ``config.ini`` whose ``table_name`` value should
            be retrieved.

        Returns
        -------
        str
            The ``table_name`` value found under the specified section.
        """
        configure = configparser.ConfigParser()
        configure.read(self.config_directory)

        return configure.get(header, 'table_name')
    
    def get_db_name(self) -> str:
        """Return the name of the database that data will be uploaded to.

        Returns
        -------
        str
            The database name from the stored connection info.
        """
        return self.db_connection_info['database']
    
    def get_site_name(self, config_key : str = "minute") -> str:
        """Return the site name derived from the configured minute-table name.

        The site name is read as the ``table_name`` value from the section
        identified by ``config_key`` in ``config.ini``.

        Parameters
        ----------
        config_key : str, optional
            Section header in ``config.ini`` that points to the minute-level
            table for the site. The ``table_name`` value of this section is
            used as the site name. Defaults to ``"minute"``.

        Returns
        -------
        str
            The site name (i.e. the ``table_name`` value for the given section).
        """
        # TODO needs an update
        configure = configparser.ConfigParser()
        configure.read(self.config_directory)

        return configure.get(config_key, 'table_name')
    
    def connect_db(self) -> [mysql.connector.MySQLConnection, mysql.connector.cursor.MySQLCursor]:
        """Create a connection to the configured MySQL database.

        Uses the host, user, password, and database name stored in
        ``db_connection_info``. Prints a message and returns ``(None, None)``
        if the connection attempt fails.

        Returns
        -------
        tuple
            A 2-tuple of ``(mysql.connector.MySQLConnection,
            mysql.connector.cursor.MySQLCursor)``. The cursor can be used to
            execute MySQL queries and the connection object can be used to
            commit those changes. Both elements are ``None`` if the connection
            could not be established.
        """

        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.db_connection_info['host'],
                user=self.db_connection_info['user'],
                password=self.db_connection_info['password'],
                database=self.db_connection_info['database']
            )
        except mysql.connector.Error:
            print("Unable to connect to database with given credentials.")
            return None, None

        print(f"Successfully connected to database.")
        return connection, connection.cursor()
    
    def connect_siteConfig_db(self) -> (mysql.connector.MySQLConnection, mysql.connector.cursor.MySQLCursor):
        """Create a connection to the ``SiteConfig`` MySQL database.

        Uses the same host, user, and password stored in ``db_connection_info``
        but always connects to the ``SiteConfig`` database regardless of the
        database name in the config file. Prints a message and returns
        ``(None, None)`` if the connection attempt fails.

        Returns
        -------
        tuple
            A 2-tuple of ``(mysql.connector.MySQLConnection,
            mysql.connector.cursor.MySQLCursor)``. The cursor can be used to
            execute MySQL queries and the connection object can be used to
            commit those changes. Both elements are ``None`` if the connection
            could not be established.
        """

        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.db_connection_info['host'],
                user=self.db_connection_info['user'],
                password=self.db_connection_info['password'],
                database="SiteConfig"
            )
        except mysql.connector.Error:
            print("Unable to connect to database with given credentials.")
            return None, None

        print(f"Successfully connected to database.")
        return connection, connection.cursor()
    
    def get_fm_token(self) -> str:
        """Retrieve a Field Manager API token using the configured credentials.

        Sends a GET request to the FieldPop login endpoint with the stored
        ``api_usr`` and ``api_pw`` credentials. Prints a message and returns
        ``None`` if the HTTP request fails or an exception is raised.

        Returns
        -------
        str or None
            The Field Manager API token string on success, or ``None`` if the
            token could not be retrieved.

        Raises
        ------
        Exception
            If ``api_usr`` or ``api_pw`` were not provided in the configuration
            file.
        """
        if self.api_usr is None or self.api_pw is None:
            raise Exception("Cannot retrieve Field Manager API token. Credentials were not provided in configuration file.")
        url = f"https://www.fieldpop.io/rest/login?username={self.api_usr}&password={self.api_pw}"
        try:
            response = requests.get(url)
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                response = response.json()  # Return the response data as JSON
                return response['data']['token']
            else:
                print(f"Failed to make GET request. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        
    def get_thingsboard_token(self) -> str:
        """Retrieve a ThingsBoard API JWT token using the configured credentials.

        Sends a POST request to the ThingsBoard Cloud login endpoint with the
        stored ``api_usr`` and ``api_pw`` credentials. Prints a message and
        returns ``None`` if the HTTP request fails or an exception is raised.

        Returns
        -------
        str or None
            The ThingsBoard JWT token string on success, or ``None`` if the
            token could not be retrieved.

        Raises
        ------
        Exception
            If ``api_usr`` or ``api_pw`` were not provided in the configuration
            file.
        """
        if self.api_usr is None or self.api_pw is None:
            raise Exception("Cannot retrieve ThingsBoard API token. Credentials were not provided in configuration file.")
        url = 'https://thingsboard.cloud/api/auth/login'

        # Request payload (data to send in the POST)
        payload = {
            'username': self.api_usr,
            'password': self.api_pw
        }

        # Headers
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        try:
            response = requests.post(url, json=payload, headers=headers)
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                response = response.json()  # Return the response data as JSON
                return response['token']
            else:
                print(f"Failed to make GET request. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        
    def get_fm_device_id(self) -> str:
        """Return the configured API device ID.

        Returns
        -------
        str
            The device ID string from the configuration file.

        Raises
        ------
        Exception
            If ``device_id`` (or ``fieldManager_device_id``) was not provided
            in the configuration file.
        """
        if self.api_device_id is None:
            raise Exception("Field Manager device ID has not been configured.")
        return self.api_device_id
    
    def get_skycentrics_token(self, request_str: str = 'GET /api/devices/ HTTP/1.', date_str: str = None) -> tuple:
        """Generate a Skycentrics HMAC-SHA1 authentication token.

        Constructs a signed token by combining the configured ``api_token``
        and a base64-encoded HMAC-SHA1 signature derived from ``api_secret``,
        the request string, the date string, and an MD5 hash of an empty body.

        Parameters
        ----------
        request_str : str, optional
            The HTTP request line used as part of the signature input
            (e.g. ``'GET /api/devices/ HTTP/1.'``). Defaults to
            ``'GET /api/devices/ HTTP/1.'``.
        date_str : str, optional
            The date string to include in the signature, formatted as
            ``'%a, %d %b %H:%M:%S GMT'``. Defaults to the current UTC time
            formatted in that style.

        Returns
        -------
        tuple
            A 2-tuple of ``(token, date_str)`` where ``token`` is the
            ``"<api_token>:<signature>"`` string and ``date_str`` is the date
            string that was used (either the supplied value or the generated
            one).
        """
        if date_str is None:
            date_str = datetime.utcnow().strftime('%a, %d %b %H:%M:%S GMT')
        signature = base64.b64encode(hmac.new(self.api_secret.encode(),
            '{}\n{}\n{}\n{}'.format(request_str, date_str, '', hashlib.md5(''.encode()).hexdigest()).encode(),
            hashlib.sha1).digest())
        token = '{}:{}'.format(self.api_token, signature.decode())
        return token, date_str
    
    def get_ls_df(self, ls_file_name: str = 'load_shift.csv') -> pd.DataFrame:
        """Load the load-shift schedule CSV and return it as a DataFrame.

        Reads the CSV file from the input directory, parses the ``date`` and
        ``startTime`` columns into a ``startDateTime`` column, and the ``date``
        and ``endTime`` columns into an ``endDateTime`` column. If the file
        does not exist, a warning is printed and an empty DataFrame is returned.

        Parameters
        ----------
        ls_file_name : str, optional
            Name of the load-shift CSV file located in the pipeline's input
            directory. Defaults to ``'load_shift.csv'``.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the load-shift schedule with additional
            ``startDateTime`` and ``endDateTime`` columns, or an empty
            DataFrame if the file does not exist.
        """
        full_ls_filename = f"{self.input_directory}load_shift.csv" 
        if ls_file_name != "" and os.path.exists(full_ls_filename):
            ls_df = pd.read_csv(full_ls_filename)
            ls_df['startDateTime'] = pd.to_datetime(ls_df['date'] + ' ' + ls_df['startTime'])
            ls_df['endDateTime'] = pd.to_datetime(ls_df['date'] + ' ' + ls_df['endTime'])
            return ls_df
        else:
            print(f"The loadshift file '{full_ls_filename}' does not exist. Thus loadshifting will not be added to daily dataframe.")
            return pd.DataFrame()
        
    def get_ls_filename(self, ls_file_name: str = 'load_shift.csv') -> str:
        """Return the full path to the load-shift CSV file if it exists.

        Constructs the full path by joining the input directory with
        ``ls_file_name``. Returns an empty string if the file does not exist
        or if ``ls_file_name`` is an empty string.

        Parameters
        ----------
        ls_file_name : str, optional
            Name of the load-shift CSV file located in the pipeline's input
            directory. Defaults to ``'load_shift.csv'``.

        Returns
        -------
        str
            Full path to the load-shift CSV file, or an empty string if the
            file does not exist.
        """
        full_ls_filename = f"{self.input_directory}{ls_file_name}" 
        if ls_file_name != "" and os.path.exists(full_ls_filename):
            return full_ls_filename
        return ""
