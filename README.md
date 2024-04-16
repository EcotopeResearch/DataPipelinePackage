# DataPipelinePackage

## To Install the Package
    From the internet for use elsewhere:
    $ pip install ecopipeline
    Install locally in an editable mode:
    Navigate to DataPipelinePackage directory and run the following command
    $ pip install -e .

## Using the Package
See https://ecotoperesearch.github.io/DataPipelinePackage/build/html/index.html for documentation

### config.ini
- database
    - user: username for host database connection 
    - password: password for host database connection
    - host: name of host 
    - database: name of database
- minute
    - table_name: name of table to be created in the mySQL database containing minute-by-minute data
- hour
    - table_name: name of table to be created in the mySQL database containing hour-by-hour data
- day
    - table_name: name of table to be created in the mySQL database containing day-by-day data
- input
    - directory: diretory of the folder containing the input files listed below
    - site_info: name of the site information csv
    - 410a_info: name of the 410a information csv
    - superheat_info: name of the superheat infomation csv
- output 
    - directory: diretory of the folder where any pipeline output should be written to
- data
    - directory: diretory of the folder from which extract loads the raw sensor data
    - fieldManager_api_usr: Username for Field Manager API if extracting data through that medium
    - fieldManager_api_pw: Password for Field Manager API if extracting data through that medium
    - fieldManager_device_id: Device ID for Field Manager API if extracting data through that medium
## Unit Testing
To run Unit tests, run the following command in the terminal in the corresponding directory:
```bash
python -m pytest
```















