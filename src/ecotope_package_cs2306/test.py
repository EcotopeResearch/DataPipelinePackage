import extract
import load
import sys
from mysql.connector import errorcode

def __main__():
    
    #extract testing
    stations = ["727935-24234"]
    #, 'KPWM', 'KSFO', 'KAVL'
    formatted_dfs = extract.get_noaa_data(['KBFI'])
    #print(formatted_dfs)
    
    #getLoadData
    config_file_path = "config.ini"
    CONFIG_ERROR_CODE = -1
    DB_ERROR_CODE = -2
    TABLE_ERROR_CODE = -3

    for key, value in formatted_dfs.items():
        #value.to_csv(f"output/{key}.csv", index=False)
        
        # get database connection information and desired table name to write data into
        config_dict = load.getLoginInfo(config_file_path)
        
        #load testing
        # check if configuration file was properly read
        if config_dict == CONFIG_ERROR_CODE:
            print(f"File path '{config_file_path}' does not exist.")
            sys.exit()

        else:
            print(f"Successfully fetched configuration information from file path {config_file_path}.")

        # establish connection to database
        db_connection, db_cursor = load.connectDB(config_dict['database'])

        # check if connection was established to database
        if db_connection == DB_ERROR_CODE:
            error = db_cursor
            if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password.")
            else:
                print(error)
            sys.exit()

        else:
            print(f"Successfully connected to {config_dict['database']['database']}.")

        # load data stored in data frame to database
        status = load.loadDatabase(db_cursor, value, config_dict['database']['database'], config_dict['table']['tablename'],
                            config_dict['table']['sitename'])

        # check if data was written to database successfully
        if status:
            print(f"Successfully wrote data frame to table {config_dict['table']['tablename']} in database "
                f"{config_dict['database']['database']}.")

        elif status == TABLE_ERROR_CODE:
            print(f"Could not create new table {config_dict['table']['tablename']} in database "
                f"{config_dict['database']['database']}")

        else:
            print(f"Unable to write data frame to table {config_dict['table']['tablename']} in database "
                f"{config_dict['database']['database']}.")

        # commit changes to database and close connection
        db_connection.commit()
        db_cursor.close()
    
    """
    #extract_json & json_to_df testing
    filenames = extract.extract_json()
    file = extract.json_to_df(filenames)
    print(file)
    """

if __name__ == '__main__':
    __main__()