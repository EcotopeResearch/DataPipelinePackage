import configparser
import os
import mysql.connector
import mysql.connector.cursor

class ConfigManager:
    """
    A helpful object to manage configuration

    Attributes
    ----------
    config_file_path : ecopipeline.ConfigManager
        The path to the config.ini file for the pipeline (e.g. "full/path/to/config.ini"). Defaults to "config.ini"
        This file should contain login information for MySQL database where data is to be loaded.
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
            self.input_directory = configure.get('input', 'directory')
        self.output_directory = output_directory
        if self.output_directory is None:
            self.output_directory = configure.get('output', 'directory')
        self.data_directory = data_directory
        if self.data_directory is None:
            self.data_directory = configure.get('data', 'directory')

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
            connection = mysql.connector.connect(self.db_connection_info)
        except mysql.connector.Error:
            print("Unable to connect to database with given credentials.")
            return None, None

        print(f"Successfully connected to database.")
        return connection, connection.cursor()