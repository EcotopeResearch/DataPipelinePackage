import unittest
import pandas as pd

class Test_Transform(unittest.TestCase):

    #CLASS DATA
    config_path = "placeholder"
    var_names_path = "input/Variable_Names.csv"

    #TEST FIXTURES BELOW
    #NOTE: Does any cleaning/resetting need to be done here? I don't think so. 

    #TEST CASES BELOW
    #NOTE: To make good tests, I will need many many pickles. Better get to work. 
    #THERE ARE FIFTEEN FUNCTIONS TO TEST! D: 

    #round_time(df)

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
    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()