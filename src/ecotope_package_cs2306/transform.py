import pandas as pd

#required input files
vars_filename = "input/vars_test.csv" 

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

    return df

#What are the select variables? Just variables with numeric inputs?
def _fillMissing(df):
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 

    Input: Pandas dataframe
    Output: Pandas dataframe
    """

    #ONLY forward fill if cumulative sum of var is not zero. 
    #ffill_length column specifies time in minutes non-changepoint vars may be forward filled if missing. 
    #vars with changepoint column set to 1, forward fill without restriction.

    #forward fill being take previous var value and insert it. we need to do this column by column.

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
    outlier_fillTest()


if __name__ == '__main__':
    __main__()