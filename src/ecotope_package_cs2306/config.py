# Sets configurations
# TODO This doesn't work
import configparser
import os
import sys
os.chdir(os.getcwd())
_config_directory = "config.ini"

if not os.path.exists(_config_directory):
    print(f"File path '{_config_directory}' does not exist.")
    sys.exit()

configure = configparser.ConfigParser()
configure.read(_config_directory)

# Directories are saved in config.ini with a relative directory to working directory
_input_directory = configure.get('input', 'directory')
_output_directory = configure.get('output', 'directory')
_data_directory = configure.get('data', 'directory')
