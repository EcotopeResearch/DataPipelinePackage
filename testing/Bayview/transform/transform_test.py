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
    def test_round_time_valid(self):
        #start with pickle fresh from extract, load into df
        rounder_df = pd.read_pickle(self.fresh_pickle_path)

        #round_time should return True if passed a df with data (correct input)
        self.assertTrue(round_time(rounder_df))
    
    def test_round_time_invalid(self):
        empty_df = pd.DataFrame()

        #round-time should return False if passed an empty df
        self.assertFalse(round_time(empty_df))


    #rename_sensors(df, var_names_path)
    def test_rename_sensors_valid(self):
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

    """ #NOTE: Issues w/testing, communicate w/team about this?
    #concat_last_row(df, last_row)
    def test_concat_last_row(self):
        #start with pickle that has had names renamed to Variable_Names (renamed.pkl)
        concat_df = pd.read_pickle("testing/Bayview/transform/pickles/renamed.pkl")
        improper_df = concat_df
        proper_df = concat_df
        sample = { 
            "col1" : [50],
            "col2" : [10], 
            "col3" : [25]
        }
        improper_last_row = pd.DataFrame(sample)
        improper_row_count = len(improper_last_row.index) + len(improper_df.index)
        proper_last_row = pd.read_pickle("testing/Bayview/transform/pickles/last_line.pkl")
        proper_row_count = len(proper_last_row.index) + len(proper_df.index)

        #this should NOT combine, as the columns do not match
        improper_df = concat_last_row(improper_df, improper_last_row)
        #this SHOULD properly combine, as it is the proper last line
        proper_df = concat_last_row(proper_df, proper_last_row)

        #assert that improper_df.rowcount != other counts, as it shoudln't append
        self.assertNotEqual(len(improper_df.index), improper_row_count)
        #assert that proper_df successfully appended, and is the size it should be
        #BUG: < not supported between instances of Timestamp and int. Catch the exception!
        self.assertEqual(len(proper_df.index), proper_row_count)
    """

    #avg_duplicate_times(df) - returns df
    def test_avg_duplicate_times_valid(self):
        averaged_df = pd.read_pickle("testing/Bayview/transform/pickles/renamed.pkl")
        #taking the first five elements and copying them on the end, so that there are duplicate entries
        averaged_df = pd.concat([averaged_df, averaged_df.head(5)])
        og_count = len(averaged_df.index)

        #NOTE: Take a typical dataframe that we know has no duplicates, make sure it doesn't remove any rows from that. 
        #function takes times w/more than one entry, averages them into one
        averaged_df = avg_duplicate_times(averaged_df)
        averaged_count = len(averaged_df.index)

        #we make sure that the count has changed and duplicates have been removed
        self.assertNotEqual(og_count, averaged_count)

    #ffill_missing(df, var_names_path) - returns df
    def test_ffill_missing_valid(self):
        #NOTE: If we add additonal testing to these functions, can check error code behavior
        pre_ffill_df = pd.read_pickle("testing/Bayview/transform/pickles/renamed.pkl")
        og_na_count = pre_ffill_df.isna().sum().sum()

        #proper ffill call, df that needs ffilling and proper path
        ffill_df = ffill_missing(pre_ffill_df, self.var_names_path)
        post_na_count = ffill_df.isna().sum().sum()

        #assert that the count of NA values in ffill_df are less than the count of NA values in pre_ffill_df
        self.assertNotEqual(og_na_count, post_na_count)
    
    def test_ffill_missing_invalid(self):
        pre_ffill_df = pd.read_pickle("testing/Bayview/transform/pickles/renamed.pkl")

        #proper ffill call, df that needs ffilling and proper path
        ffill_df = ffill_missing(pre_ffill_df, self.var_names_path)
        post_na_count = ffill_df.isna().sum().sum()
        #ffill called on a df that is fully filled, assert no changes were made
        triple_df = ffill_missing(ffill_df, self.var_names_path)
        triple_na_count = triple_df.isna().sum().sum()

        self.assertEqual(post_na_count, triple_na_count)


    #sensor_adjustment(df) - returns df
    #BUG: Something with how time is extracted in this function is currently broken, that bug needs to be fixed first. 
    def test_sensor_adjustment_valid(self):
        unadjusted_df = pd.read_pickle("testing/Bayview/transform/pickles/ffilled.pkl")
        og_df = unadjusted_df #make this a deep copy!!
        
        #Eventually, adjusted_df should be loaded with a df that NEEDS adjustment. HOW CAN I GET THIS?
        #adjusted_df = pd.DataFrame() 
        #test to make sure it has modified the df when that df has an offset
        #adjusted_df = sensor_adjustment(adjusted_df)

        #test to make sure a df without an offset does not get modified
        unadjusted_df = sensor_adjustment(unadjusted_df) 

        #check that the dataframe is the exact same, #adjusted df is pending!
        self.assertTrue(unadjusted_df.equals(og_df))

    def test_sensor_adjustment_invalid(self):
        #empty df to make sure it doesn't break
        sensor_adjustment(pd.DataFrame())

    #get_energy_by_min(df) - returns df
    def test_get_energy_by_min_valid(self):
        get_energy_df = pd.read_pickle("testing/Bayview/transform/pickles/ffilled.pkl")
        #df of the original energy vars to compare with after function is run
        og_energy_vars = get_energy_df.filter(regex=".*Energy.*")
        og_energy_vars = og_energy_vars.filter(regex=".*[^BTU]$")

        #ideal function call, assert that the dataframe has properly updated only energy values
        avg_energy_df = get_energy_by_min(get_energy_df)
        #extract only energy vars
        energy_vars = avg_energy_df.filter(regex=".*Energy.*")
        energy_vars = energy_vars.filter(regex=".*[^BTU]$")

        #assert that get_energy_df and avg_energy_df have different values for their energy vars
        self.assertNotEqual(og_energy_vars.iloc[:, 1].sum(), energy_vars.iloc[:, 1].sum())

    def test_get_energy_by_min_invalid(self):
        #load improper input w/empty df, make sure it doesn't break
        empty_df = get_energy_by_min(pd.DataFrame())

    #NOTE: Roger
    #verify_power_energy(df) 
    def test_verify_power_energy(self):
        verify_power_energy(pd.DataFrame())

        no_energy = {'Power': [1, 2], 'Power2': [20,30], 'More_Power': [10, 20], 'PowerEnergy': [3, 4]}
        no_energy_df = pd.DataFrame(data=no_energy)
        verify_power_energy(no_energy_df)
        
        no_power = {'Energy': [1, 2], 'More_Energy': [20,30], 'Meter_SkidAux_Energy': [10, 20], 'Energy2': [3, 4]}
        no_power_df = pd.DataFrame(data=no_power)
        verify_power_energy(no_power_df)

    #remove_outliers(df, var_names_path) - returns df
    def test_remove_outliers_valid(self):
        outlier_df = pd.read_pickle("testing/Bayview/transform/pickles/energy_by_min.pkl")
        #recording NA count of original DF to make note that values have pruned
        og_na_count = outlier_df.isna().sum().sum()

        #NOTE: Instead of energy_by_min.pkl, we could use a sample that just has BIG entries
        #test on proper df, make sure out of bounds values were removed
        pruned_df = remove_outliers(outlier_df, self.var_names_path)
        pruned_na_count = pruned_df.isna().sum().sum()

        self.assertNotEqual(outlier_df.isna().sum().sum(), og_na_count)
        #assert that the number of NA values are different in the original df and the removed df
        self.assertNotEqual(pruned_na_count, og_na_count)
    
    def test_remove_outliers_invalid(self):
        #test on an empty df, make sure nothing breaks
        empty_df = remove_outliers(pd.DataFrame(), self.var_names_path)
        pass

    """
    #NOTE: Roger
    #calculate_cop_values(df, heatLoss_fixed, thermo_slice) - returns cop_values df
    def test_calculate_cop_values(self):
        cop_values = pd.read_pickle("testing/Bayview/transform/pickles/cop_values.pkl")
        pruned = pd.read_pickle("testing/Bayview/transform/pickles/pruned_outliers.pkl")

        calculate_cop_values(pd.DataFrame(), 27.278, "6:00AM") 

        cop_df = calculate_cop_values(pruned)

        self.assertEqual(list(cop_values.columns), list(cop_df.columns))
    """
        
    #aggregate_df(df) - returns hourly_df and daily_df
    def test_aggregate_df_valid(self):
        #NOTE: This is potentially a candidate for extra testing
        unaggregated_df = pd.read_pickle("testing/Bayview/transform/pickles/pruned_outliers.pkl")

        #proper call to function returns our hourly and daily test dfs
        hourly_df, daily_df = aggregate_df(unaggregated_df)

        #test that hourly_df returns properly (fewer rows than unaggregated_data)
        self.assertTrue(len(unaggregated_df.index) > len(hourly_df.index))
        #test that daily_df returns properly (fewer rows than hourly_df AND has load_shift_day as a column!)
        self.assertTrue(len(hourly_df.index) > len(daily_df.index))
        self.assertTrue(("load_shift_day" in daily_df.columns))

    def test_aggregate_df_invalid(self):
        #empty df, make sure nothing breaks
        aggregate_df(pd.DataFrame())

    #NOTE: Roger
    #get_temp_zones120(df) - returns df
    def test_get_temp_zones120(self):
        temp_zones_df = pd.read_pickle("testing/Bayview/transform/pickles/temp_zones.pkl")
        get_temp_zones120(pd.DataFrame())

        temp_zones_df = get_temp_zones120(temp_zones_df)
        zones = ['Temp_top', 'Temp_midtop', 'Temp_mid', 'Temp_midbottom', 'Temp_bottom']
        self.assertEqual(zones, list(temp_zones_df.iloc[:, -5:].columns))

        test = {'Temp1': [1, 2], 'Temp1_stuff': [3, 4], 'Temp_1stuff': [5, 6], 'Temp.1stuff': [7, 8]}

        test_df = pd.DataFrame(data=test)
        test_df = get_temp_zones120(test_df)

        expected1 = 2.0
        expected2 = 3.0

        self.assertEqual(expected1, test_df['Temp_top'][0])
        self.assertEqual(expected2, test_df['Temp_top'][1])
        
    """
   #NOTE: Roger
    #get_storage_gals120(df) - returns df
    def test_get_storage_gals120(self):
        storage_gals120_df = pd.read_pickle("testing/Bayview/transform/pickles/storage_gals.pkl")
        get_storage_gals120(pd.DataFrame())

        storage_gals120_df = get_storage_gals120(storage_gals120_df)
        gals120_columns = ['Vol120', 'ZoneTemp120', 'Vol_Equivalent_to_120']
        self.assertEqual(gals120_columns,list(storage_gals120_df.iloc[:, -3:].columns))
    """    
    
    #join_to_hourly(hourly_df, noaa_df) - returns hourly_df
    def test_join_to_hourly_valid(self):
        #pickle of hourly_df, and pickle of NOAA data
        hourly_df = pd.read_pickle("testing/Bayview/transform/pickles/hourly_df.pkl")
        noaa_df = pd.read_pickle("testing/Bayview/transform/pickles/noaa_df.pkl")
        #combined_count = len(hourly_df.columns) + len(noaa_df.columns)
        og_count = len(hourly_df.columns)

        #properly call, join these together, make sure that columns were added on
        combined_df = join_to_hourly(hourly_df, noaa_df)

        #assert that the column count has increased after join_to_hourly was called
        self.assertTrue(len(combined_df.columns) > og_count)

    def test_join_to_hourly_invalid(self):
        #pass in both dataframes to be merged w/nothing, see if it crashes
        join_to_hourly(pd.DataFrame(), pd.DataFrame())

    #join_to_daily(daily_df, cop_values) - returns daily_df
    def test_join_to_daily_valid(self):
        daily_df = pd.read_pickle("testing/Bayview/transform/pickles/daily_df.pkl")
        cop_values = pd.read_pickle("testing/Bayview/transform/pickles/cop_values.pkl")
        og_count = len(daily_df.columns)

        #pass in two empty dfs, or have just daily missing or just cop_values missing
        combined_df = join_to_daily(daily_df, cop_values)

        #proper call, make sure that cop_values columns were added on 
        combined_df = join_to_daily(daily_df, cop_values)

        #assert that the column count has increased after join_to_daily was called
        self.assertTrue(len(combined_df.columns) > og_count)

    def test_join_to_daily_invalid(self):
        #pass in both dataframes as empty, see if it breaks
        join_to_daily(pd.DataFrame(), pd.DataFrame())

if __name__ == '__main__':
    """
    # NOTE: concat testing!!
    renamed_data = pd.read_pickle("testing/Bayview/transform/pickles/renamed.pkl")
    print("\n\nNumber of rows BEFORE concat: ", len(renamed_data.index), "\n\n")
    proper_line = pd.read_pickle("testing/Bayview/transform/pickles/last_line.pkl")

    print(len(renamed_data.columns))
    print(len(proper_line.columns))
    concat_last_row(renamed_data, proper_line)

    print("\n\nNumber of rows AFTER concat: ", len(renamed_data.index), "\n\n")
    """

    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()