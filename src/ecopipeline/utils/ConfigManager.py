import configparser
import os
import mysql.connector
import mysql.connector.cursor
import requests

class ConfigManager:
    """
    A helpful object to manage configuration

    Attributes
    ----------
    config_file_path : str
        The path to the config.ini file for the pipeline (e.g. "full/path/to/config.ini"). Defaults to "config.ini"
        This file should contain login information for MySQL database where data is to be loaded.
    input_directory : str
        The path to the input directory for the pipeline (e.g. "full/path/to/pipeline/input/"). 
        Defaults to the input directory defined in the config.ini configuration file
    output_directory : str
        The path to the output directory for the pipeline (e.g. "full/path/to/pipeline/output/"). 
        Defaults to the output directory defined in the config.ini configuration file
    data_directory : str
        The path to the data directory for the pipeline (e.g. "full/path/to/pipeline/data/"). 
        Defaults to the data directory defined in the config.ini configuration file
    eco_file_structure : boolean
        Set to True if this is a data pipeline running on Ecotope's server for file path reconfiguration. Set False if not running at Ecotope.
        Defaults to False
    """
    def __init__(self, config_file_path : str = "config.ini", input_directory : str = None, output_directory : str = None, data_directory : str = None, eco_file_structure : bool = False):
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
        self.api_device_id = None
        if self.data_directory is None:
            configured_data_method = False
            if 'data' in configure:
                if 'directory' in configure['data']:
                    self.data_directory = configure.get('data', 'directory')
                    configured_data_method = True
                if 'fieldManager_api_usr' in configure['data'] and 'fieldManager_api_pw' in configure['data'] and 'fieldManager_device_id' in configure['data']:
                    self.api_usr = configure.get('data', 'fieldManager_api_usr')
                    self.api_pw = configure.get('data', 'fieldManager_api_pw')
                    self.api_device_id = configure.get('data','fieldManager_device_id')
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
        """
        Returns path to the full path to the Variable_Names.csv file.
        This file should be in the pipeline's input directory "/" (i.e. "full/path/to/pipeline/input/Variable_Names.csv")
        """
        return f"{self.input_directory}Variable_Names.csv"

    def get_weather_dir_path(self) -> str:
        """
        Returns path to the directory that holds NOAA weather data files.
        This diectory should be in the pipeline's data directory "/" (i.e. "full/path/to/pipeline/data/weather")
        """
        return f"{self.data_directory}weather"
    
    def get_db_table_info(self, table_headers : list) -> dict:
        """
        Reads the config.ini file stored in the config_file_path file path.   

        Parameters
        ---------- 
        table_headers : list
            A list of table headers. These headers must correspond to the 
            section headers in the config.ini file. Your list must contain the section
            header for each table you wish to write into. The first header must correspond 
            to the login information of the database. The other are the tables which you wish
            to write to.

        Returns
        ------- 
        dict: 
            A dictionary containing all relevant information is returned. This
            includes information used to create a connection with a mySQL server and
            information (table names and column names) used to load the data into 
            tables. 
        """

        configure = configparser.ConfigParser()
        configure.read(self.config_directory)

        db_table_info = {header: {"table_name": configure.get(header, 'table_name')} for header in table_headers}
        db_table_info["database"] = self.db_connection_info["database"]

        print(f"Successfully fetched configuration information from file path {self.config_directory}.")
        return db_table_info
    
    def get_db_name(self):
        """
        returns name of database that data will be uploaded to
        """
        return self.db_connection_info['database']
    
    def get_site_name(self):
        """
        returns name of site
        """
        # TODO needs an update
        configure = configparser.ConfigParser()
        configure.read(self.config_directory)

        return configure.get("minute", 'table_name')
    
    def connect_db(self) -> (mysql.connector.MySQLConnection, mysql.connector.cursor.MySQLCursor):
        """
        Create a connection with the mySQL server. 

        Parameters
        ----------  
        None

        Returns
        ------- 
        mysql.connector.MySQLConnection, mysql.connector.cursor.MySQLCursor: 
            A connection and cursor object. THe cursor can be used to execute
            mySQL queries and the connection object can be used to save those changes. 
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
    
    def get_fm_token(self) -> str:
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
        
    def get_fm_device_id(self) -> str:
        if self.api_device_id is None:
            raise Exception("Field Manager device ID has not been configured.")
        return self.api_device_id