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

    """
    #concat_last_row(df, last_row)
    #BUG: Should be fixed once last_line.pkl is updated and we have a proper line with 73 columns!
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
    def test_avg_duplicate_times(self):
        #NOTE: eventually, use concat.pkl, for now, use renamed.pkl
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
    def test_ffill_missing(self):
        #NOTE: eventually, use averaged.pkl, for now, use renamed.pkl
        #NOTE: If we add additonal testing to these functions, can check error code behavior
        pre_ffill_df = pd.read_pickle("testing/Bayview/transform/pickles/renamed.pkl")
        og_na_count = pre_ffill_df.isna().sum().sum()

        #proper ffill call, df that needs ffilling and proper path
        ffill_df = ffill_missing(pre_ffill_df, self.var_names_path)
        post_na_count = ffill_df.isna().sum().sum()

        #ffill called on a df that is fully filled, assert no changes were made
        triple_df = ffill_missing(ffill_df, self.var_names_path)
        triple_na_count = triple_df.isna().sum().sum()

        #assert that the count of NA values in ffill_df are less than the count of NA values in pre_ffill_df
        self.assertNotEqual(og_na_count, post_na_count)
        #assert that no changes were made when ffill is called on a df that was already ffill'd 
        self.assertEqual(post_na_count, triple_na_count)

    """
    #sensor_adjustment(df) - returns df
    #BUG: Carlos mentioned there was an issue with this, update something in the function
    def test_sensor_adjustment(self):
        #NOTE: Update pickle after other functions are fixed! For now use ffilled.pkl
        unadjusted_df = pd.read_pickle("testing/Bayview/transform/pickles/ffilled.pkl")

        #Just because this function is confusing and others don't really depend on it, I'm delaying writing tests for it
        pass
    """

    #get_energy_by_min(df) - returns df
    def test_get_energy_by_min(self):
        #NOTE: update pickle to sensor_adjusted.pkl, for now use ffilled.pkl
        get_energy_df = pd.read_pickle("testing/Bayview/transform/pickles/ffilled.pkl")
        #df of the original energy vars to compare with after function is run
        og_energy_vars = get_energy_df.filter(regex=".*Energy.*")
        og_energy_vars = og_energy_vars.filter(regex=".*[^BTU]$")

        #testing that function does not break when passed improper input, like an empty df
        empty_df = get_energy_by_min(pd.DataFrame())

        #ideal function call, assert that the dataframe has properly updated only energy values
        avg_energy_df = get_energy_by_min(get_energy_df)
        #extract only energy vars
        energy_vars = avg_energy_df.filter(regex=".*Energy.*")
        energy_vars = energy_vars.filter(regex=".*[^BTU]$")

        #assert that get_energy_df and avg_energy_df have different values for their energy vars
        self.assertNotEqual(og_energy_vars.iloc[:, 1].sum(), energy_vars.iloc[:, 1].sum())

    #NOTE: Roger
    #verify_power_energy(df) 

    #remove_outliers(df, var_names_path) - returns df
    def test_remove_outliers(self):
        outlier_df = pd.read_pickle("testing/Bayview/transform/pickles/energy_by_min.pkl")
        #recording NA count of original DF to make note that values have pruned
        og_na_count = outlier_df.isna().sum().sum()

        #test on an empty df, make sure nothing breaks
        empty_df = remove_outliers(pd.DataFrame(), self.var_names_path)

        #NOTE: Instead of energy_by_min.pkl, we could use a sample that just has BIG entries
        #test on proper df, make sure out of bounds values were removed
        pruned_df = remove_outliers(outlier_df, self.var_names_path)
        pruned_na_count = pruned_df.isna().sum().sum()

        self.assertNotEqual(outlier_df.isna().sum().sum(), og_na_count)
        #assert that the number of NA values are different in the original df and the removed df
        self.assertNotEqual(pruned_na_count, og_na_count)

    #NOTE: Roger
    #calculate_cop_values(df, heatLoss_fixed, thermo_slice) - returns cop_values df

    #aggregate_df(df) - returns hourly_df and daily_df
    def test_aggregate_df(self):
        #NOTE: This is a big candidate for extra testing
        unaggregated_df = pd.read_pickle("testing/Bayview/transform/pickles/pruned_outliers.pkl")

        #test w/an empty df that nothing explodes
        aggregate_df(pd.DataFrame())

        #proper call to function returns our hourly and daily test dfs
        hourly_df, daily_df = aggregate_df(unaggregated_df)

        #test that hourly_df returns properly (fewer rows than unaggregated_data)
        self.assertTrue(len(unaggregated_df.index) > len(hourly_df.index))
        #test that daily_df returns properly (fewer rows than hourly_df AND has load_shift_day as a column!)
        self.assertTrue(len(hourly_df.index) > len(daily_df.index))
        self.assertTrue(("load_shift_day" in daily_df.columns))

    #NOTE: Roger
    #get_temp_zones120(df) - returns df

    #NOTE: Roger
    #get_storage_gals120(df) - returns df

    #join_to_hourly(hourly_df, noaa_df) - returns hourly_df
    def test_join_to_hourly(self):
        #pickle of hourly_df, and pickle of NOAA data
        hourly_df = pd.read_pickle("testing/Bayview/transform/pickles/hourly_df.pkl")
        noaa_df = pd.read_pickle("testing/Bayview/transform/pickles/noaa_df.pkl")
        #combined_count = len(hourly_df.columns) + len(noaa_df.columns)
        og_count = len(hourly_df.columns)

        #pass in two empty dfs, or just hourly missing and just noaa missing
        join_to_hourly(pd.DataFrame(), pd.DataFrame())

        #properly call, join these together, make sure that columns were added on
        combined_df = join_to_hourly(hourly_df, noaa_df)

        #assert that the column count has increased after join_to_hourly was called
        self.assertTrue(len(combined_df.columns) > og_count)

    #join_to_daily(daily_df, cop_values) - returns daily_df
    def test_join_to_daily(self):
        daily_df = pd.read_pickle("testing/Bayview/transform/pickles/daily_df.pkl")
        cop_values = pd.read_pickle("testing/Bayview/transform/pickles/cop_values.pkl")
        og_count = len(daily_df.columns)

        #pass in two empty dfs, or have just daily missing or just cop_values missing
        combined_df = join_to_daily(daily_df, cop_values)

        #proper call, make sure that cop_values columns were added on 
        combined_df = join_to_daily(daily_df, cop_values)

        #assert that the column count has increased after join_to_daily was called
        self.assertTrue(len(combined_df.columns) > og_count)


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

    """
    #pruned_outliers.pkl
    pruned_data_df = pd.read_pickle("testing/Bayview/transform/pickles/pruned_data.pkl")
    cop_calc_data = 

    pd.to_pickle(, "testing/Bayview/transform/pickles/pruned_outliers.pkl")
    #27.278, "6:00AM"
    """

    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()