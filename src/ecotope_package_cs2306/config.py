# Sets configurations
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

directories = [_input_directory, _output_directory, _data_directory]
for directory in directories:
    if not os.path.isdir(directory):
        print(f"File path '{directory}' does not exist, creating directory")
        os.makedirs(directory)
