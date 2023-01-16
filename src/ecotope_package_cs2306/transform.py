import pandas as pd

#required input files
vars_filename = "input/vars_test.csv" #currently set to a test until real csv is completed


def renameVariables(df: pd.DataFrame):
    # import configuration file which stores important info about each variable
    data_info = pd.read_csv("input/Variable_Names.csv")

    # slice on the relevant columns
    data_info = data_info[1:87]

    # store variable alias name and true name in list
    variable_alias = list(data_info["variable_alias"])
    variable_true = list(data_info["variable_name"])

    # create a dict in format {{'variable.alias': 'variable.true'}, ...}
    variable_alias_true_dict = dict(zip(variable_alias, variable_true))

    print(variable_alias_true_dict)

    # replace alias variable names with true variable names
    # df.replace(variable_alias_true_dict)

    return df


#STRETCH GOAL
#Functionality for alarms being raised based on bounds needs to happen here. 
#Try and keep things clean so that can be added later. 
def _removeOutliers(df, vars_filename):
    """
    Function will take the location of extracted data in a json and bounds information in a csv,
    store the json data in a pandas dataframe, then remove outliers above or below bounds as 
    designated by the csv. Function then returns the resulting dataframe. 

    Input: Pandas dataframe and file location of variable processing information
    Output: Pandas dataframe 
    """
    #Bounds setup df
    #Variable_Names.csv, only keeps variable_name, lower_bound, and upper_bound columns, and only if bounds exist
    bounds_df = pd.read_csv(vars_filename)
    bounds_df = bounds_df.loc[:, ["variable_name", "lower_bound", "upper_bound"]]
    bounds_df.dropna(axis=0, thresh=2, inplace=True)
    print(bounds_df)

    #Removal
    #Compare the two dataframes. For each row of the data, locate column names with names in the rows
    #of the bounds df. If matched, go all the way down that column, and remove the data IF it is not
    #within the set bounds indicated by bounds df col 2 and 3. 

    #basically, call dropna but for deleting individual entires?
    for var in df: #For each column
        #upper = 
        #lower = 
        #loc column names that match
        #once located, if within bounds for each in that column
        #pd.NA to replace?
        pass

    return df

#What are the select variables? Just variables with numeric inputs?
def _fillMissing(df):
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 

    Input: Pandas dataframe
    Output: Pandas dataframe
    """

    #ONLY forward fill if cumulative sum of var is not zero, e.g. don't do it until you find at least one valid entry first.
    #ffill_length column specifies time in minutes non-changepoint vars may be forward filled if missing. 
    #vars with changepoint column set to 1, forward fill without restriction. obv cumulative sum still applies.

    #forward fill being take previous var value and insert it. we need to do this column by column.
    #probably use pd.ffill

    return df


#Test function for simple main, will be removed once transform.py is complete
def outlier_fillTest():
    #Sample df, this should come from extract in actual running
    testdf_filename = "input/csv_test.csv"
    df = pd.read_csv(testdf_filename)

    print("\nTesting _removeOutliers...\n")
    _removeOutliers(df, vars_filename)
    print("\nFinished testing _removeOutliers\n")

    #print("\nTesting _fillMissing...\n")


#Test main, will be removed once transform.py is complete
def __main__():
    # outlier_fillTest()
    df_path = "input/ecotope_wide_data_original.csv"
    ecotope_data = pd.read_csv(df_path)
    ecotope_data.set_index("time", inplace=True)

    ecotope_data = renameVariables(ecotope_data)

    # print(ecotope_data)


if __name__ == '__main__':
    __main__()