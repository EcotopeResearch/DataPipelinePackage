from src.ecotope_package_cs2306 import extract
from src.ecotope_package_cs2306 import load


def __main__():
    stations = ["727935-24234"]
    formatted_dfs = extract.get_noaa_data(['KBFI'])
    #print(formatted_dfs)

    config_file_path = "config.ini"
    for key, value in formatted_dfs.items():
        # get database connection information and desired table name to write data into
        config_dict = load.getLoginInfo(config_file_path)

        # establish connection to database
        db_connection, db_cursor = load.connectDB(config_dict['database'])

        # load data stored in data frame to database
        load.loadDatabase(db_cursor, value, config_dict['database']['database'], config_dict['table']['tablename'],
                            config_dict['table']['sitename'])

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