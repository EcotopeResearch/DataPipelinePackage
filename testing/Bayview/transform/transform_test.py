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
        rounder_df = pd.read_pickle(self.fresh_pickle_path)
        empty_df = pd.DataFrame()

        trueBool = round_time(rounder_df)
        falseBool = round_time(empty_df)

        self.assertEqual(trueBool, True)
        self.assertEqual(falseBool, False)

    #rename_sensors(df, var_names_path)

    #concat_last_row(df, last_row) 

    #avg_duplicate_times(df) - returns df

    #ffill_missing(df, var_names_path) - returns df

    #sensor_adjustment(df) - returns df

    #get_energy_by_min(df) - returns df

    #verify_power_energy(df) 

    #remove_outliers(df, var_names_path) - returns df

    #calculate_cop_values(df, heatLoss_fixed, thermo_slice) - returns cop_values df

    #aggregate_df(df) - returns hourly_df and daily_df

    #get_temp_zones120(df) - returns df

    #get_storage_gals120(df) - returns df

    #join_to_hourly(hourly_df, noaa_df) - returns hourly_df

    #join_to_daily(daily_df, cop_values) - returns daily_df

    def test_fail(self):
        self.assertEqual(3, 5)
    def test_pass(self):
        self.assertEqual(5, 5)

if __name__ == '__main__':
    """
    fresh_df = pd.read_pickle("testing/Bayview/transform/pickles/trans_start.pkl")
    print(fresh_df)
    print("I am in main, yay!!")
    #runs test_xxx functions, shows what passed or failed. 
    """
    unittest.main()