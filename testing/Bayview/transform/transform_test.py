import unittest
import pandas as pd
from ecotope_package_cs2306 import rename_sensors, avg_duplicate_times, remove_outliers, ffill_missing, sensor_adjustment, get_energy_by_min, verify_power_energy, calculate_cop_values, round_time, aggregate_df, join_to_hourly, join_to_daily, concat_last_row, get_temp_zones120, get_storage_gals120

class Test_Transform(unittest.TestCase):

    #CLASS DATA
    var_names_path = "input/Variable_Names.csv"
    fresh_pickle_path = "testing/Bayview/transform/pickles/trans_start.pkl"

    #TEST FIXTURES BELOW
    #NOTE: Does any cleaning/resetting need to be done here? I don't think so. 

    #TEST CASES BELOW
    #NOTE: To make good tests, I will need many many pickles. Better get to work. 
    #THERE ARE FIFTEEN FUNCTIONS TO TEST! D: 

    #round_time(df)
    def test_round_time(self):
        #start with pickle fresh from extract
        rounder_df = pd.read_pickle(self.fresh_pickle_path)
        empty_df = pd.DataFrame()

        #round_time should return True if passed a df with data (correct input)
        trueBool = round_time(rounder_df)
        #round_Time should return False if passed an empty df 
        falseBool = round_time(empty_df)

        self.assertEqual(trueBool, True)
        self.assertEqual(falseBool, False)

    #rename_sensors(df, var_names_path)
    def test_rename_sensors(self):
        #start with pickle that has had data rounded from round_time(df)
        rename_df = pd.read_pickle("testing/Bayview/transform/pickles/rounded.pkl")
        #Making a series with desired var_names, this is what the columns should be renamed to!
        var_data = pd.read_csv(self.var_names_path)
        var_names = var_data["variable_name"].tolist()
        var_names.pop(0) #first entry is nan

        #correct input, function returns nothing. we take the columns names as a list to compare
        rename_sensors(rename_df)
        renamed_names = rename_df.columns.tolist()

        #See if first few elements in each list match
        self.assertEqual(renamed_names[:5], var_names[:5]) 

    #concat_last_row(df, last_row)
    def test_concat_last_row(self):
        #all this function does is get passed in a df, and a row, and combines them. easy test.

        pass

    #avg_duplicate_times(df) - returns df

    #ffill_missing(df, var_names_path) - returns df

    #sensor_adjustment(df) - returns df

    #get_energy_by_min(df) - returns df

    #NOTE: Roger
    #verify_power_energy(df) 

    #remove_outliers(df, var_names_path) - returns df

    #NOTE: Roger
    #calculate_cop_values(df, heatLoss_fixed, thermo_slice) - returns cop_values df

    #aggregate_df(df) - returns hourly_df and daily_df

    #NOTE: Roger
    #get_temp_zones120(df) - returns df

    #NOTE: Roger
    #get_storage_gals120(df) - returns df

    #join_to_hourly(hourly_df, noaa_df) - returns hourly_df

    #join_to_daily(daily_df, cop_values) - returns daily_df

    """
    #tests to make sure I set up the class correctly, done with
    def test_fail(self):
        self.assertEqual(3, 5)
    def test_pass(self):
        self.assertEqual(5, 5)
    """

if __name__ == '__main__':
    """
    rename_df = pd.read_pickle("testing/Bayview/transform/pickles/rounded.pkl")
    var_data = pd.read_csv("input/Variable_Names.csv")
    #this is a series of the names
    var_data = var_data["variable_name"].tolist()
    #NOTE: var_data's first element is nan, pop it?
    var_data.pop(0)
    rename_sensors(rename_df, "input/Variable_Names.csv")
    rename_names = rename_df.columns.tolist()
    print(var_data)
    print("Printed var data type!!")
    #this is an index w/the names?
    print(rename_names)
    print("Printed column names type!!")

    #NOTE: These lists are not identical, but VERY close. How can I test this?
    if(var_data == rename_names):
        print("The lists match! Nice!")
    """
    
    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()