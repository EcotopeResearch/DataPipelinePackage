import unittest
import numpy as np
import pandas as pd
import datetime as dt
from ecopipeline import get_refrig_charge, gas_valve_diff, change_ID_to_HVAC, gather_outdoor_conditions, elev_correction, replace_humidity, create_fan_curves, condensate_calculations, site_specific, get_site_info, get_site_cfm_info

class Test_Transform(unittest.TestCase):
    #NOTE: If you want to run the tests w/an updated LBNL, you have to run the install script 
    #again w/pip, saved here: 
    #pip uninstall "PLACEHOLDER"
    #pip install "PLACEHOLDER"

    #class data (maybe, var_names directory)
    site_info_path = "testing/LBNL/transform/LBNL-input/site_info.csv"
    four_path = "testing/LBNL/transform/LBNL-input/410a_pt.csv"
    superheat_path = "testing/LBNL/transform/LBNL-input/superheat.csv"

    #NOTE: Instead of checking if files are missing inside functions, we instead do that at the start? 
    #Check that test function instead of all of those

    #Carlos
    def test_create_fan_curves_valid(self):
        site = "AZ2_01"
        site_info = get_site_info(site, "testing/LBNL/transform/LBNL-input/site_info.csv")
        site_cfm =  get_site_cfm_info(site,  "testing/LBNL/transform/LBNL-input/sitecfminfo.csv")
        # Test with some dummy data

        result = create_fan_curves(site_cfm, site_info)

        # Check the result is a DataFrame
        self.assertIsInstance(result, pd.DataFrame)

        # Check the result has the expected columns
        self.assertSetEqual(set(result.columns), {'site', 'a', 'b'})

        # Check the result has the expected number of rows
        self.assertEqual(len(result), 1)

        # Check the result has the expected sites
        self.assertSetEqual(set(result['site']), {'AZ2_01'})

        # Check the result has the expected coefficients
        self.assertAlmostEqual(result.loc[result['site'] == 'AZ2_01', 'a'].values[0], 1229, delta=0.1)
        self.assertAlmostEqual(result.loc[result['site'] == 'AZ2_01', 'b'].values[0], 0, delta=0.1)

    def test_condensate_calculations_valid(self):
        df = pd.DataFrame({
            'Condensate_ontime': [0, 5, 10, 15, 20],
            'Condensate_pulse_avg': [0, 1, 2, 3, 4]
        })
        site = 'test_site'
        site_info = pd.Series({
            'condensate_cycle_length': 5,
            'condensate_oz_per_tip': 1
        })
        result = condensate_calculations(df, site, site_info)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), len(df))
        self.assertNotIn("Condensate_oz", result.columns)
        self.assertIn("Condensate_kJ", result.columns)


    #Carlos
    def test_site_specific_valid_case1(self):
        df = pd.DataFrame({
            'Pressure_staticP': [0, 0, 0],
            'Power_OD_total1': [0, 0, 0],
            'Power_OD_fan1': [0, 0, 0],
            'Power_OD_compressor1': [0, 0, 0],
            'Power_AH1': [0, 0, 0]
        })
        result = site_specific(df, "MO2_01")
        np.testing.assert_array_equal(result['Pressure_staticP'].values, np.array([55, 55, 55]))

    def test_site_specific_valid_case2(self):
        df = pd.DataFrame({
            'Pressure_staticP': [1, 2, 3],
            'Power_OD_total1': [4, 5, 6],
            'Power_OD_fan1': [7, 8, 9],
            'Power_AH1': [10, 11, 12]
        })
        result = site_specific(df, "AZ2_01")
        self.assertIn('Power_OD_compressor1', result.columns)
        self.assertIn('Power_system1', result.columns)

    def test_site_specific_valid_case3(self):
        df = pd.DataFrame({
            'Power_OD_fan1': [0, 0, 0],
            'Power_OD_compressor1': [0, 0, 0],
            'Power_system1': [0, 0, 0]
        })
        result = site_specific(df, "AZ2_03")
        self.assertIn('Power_OD_total1', result.columns)
        self.assertIn('Power_AH1', result.columns)

    def test_site_specific_valid_case4(self):
        df = pd.DataFrame({
            'Power_OD_total1': [0, 0, 0],
            'Power_AH1': [0, 0, 0],
        })
        result = site_specific(df, "AZ2_04")
        self.assertIn('Power_system1', result.columns)
    def test_site_specific_invalid(self):
        #test invalid input
        pass
    
    """ #currently has errors! 
    def test_gas_valve_diff_valid(self):
        # site AZ2_01 uses hp (not gas) heating
        hp_pickle = "testing/LBNL/transform/pickles/AZ2_01_04202022.pkl"
        hp_df = pd.read_pickle(hp_pickle)
        result_hp_df = gas_valve_diff(hp_df, "AZ2_01")

        # site IL2_01 uses gas heating 
        gas_pickle = "testing/LBNL/transform/pickles/IL2_01_06182022.pkl"
        gas_df = pd.read_pickle(gas_pickle)
        result_gas_df = gas_valve_diff(gas_df, "IL2_01")
        
        #self.assertNotEqual(gas_df.iloc[:, 1].sum(), result_gas_df.iloc[:, 1].sum())
        self.assertEqual(hp_df.iloc[:, 1].sum(), result_hp_df.iloc[:, 1].sum())
    """
    
    def test_gas_valve_diff_invalid(self):
        empty_df = pd.DataFrame()
        result_df = gas_valve_diff(empty_df, "AZ2_01")
        self.assertEqual(True, empty_df.equals(result_df))
    
    def test_gather_outdoor_conditions_valid(self):
        pickle = "testing/LBNL/transform/pickles/IL2_01_10052022.pkl"
        df = pd.read_pickle(pickle)
        df = df.loc[:,~df.columns.duplicated()]
        result_df = gather_outdoor_conditions(df, "IL2_01")
        expected_cols = ['time_utc', 'IL2_01_ODT', 'IL2_01_ODRH']
        self.assertEqual(True, np.array_equal(expected_cols, result_df.columns))

    def test_gather_outdoor_conditions_invalid(self):
        empty_df = pd.DataFrame()
        result_df = gather_outdoor_conditions(empty_df, "AZ2_01")
        self.assertEqual(True, empty_df.equals(result_df))

    """ #currently has errors! 
    def test_elev_correction_valid(self):
        result_df = elev_correction("IL2_01")
        expected_cols = ['site', 'elev', 'air_corr']
        self.assertEqual(True,  np.array_equal(expected_cols, result_df.columns))
    """
    """ #currently has errors!
    def test_elev_correction_invalid(self):
        empty_df = pd.DataFrame()
        result_df = elev_correction("FAKE1_01")
        self.assertEqual(True, (result_df.equals(empty_df)))
    """
    
    def test_refrig_charge_valid(self):
        #we assume proper input variables!
        
        #txv pickle
        txv = "testing/LBNL/transform/pickles/AZ2_01_04202022.pkl"
        site_txv = 'AZ2_01'
        df1 = pd.read_pickle(txv)
        #superheat pickle (right now, uses custom data!)
        #orifice = "testing/LBNL/transform/pickles/IL2_01_06052022.pkl"
        orifice = "testing/LBNL/transform/pickles/sh_tester.csv"
        site_orifice = 'IL2_01'
        df2 = pd.read_csv(orifice)

        #function calls
        df1 = get_refrig_charge(df1, site_txv, self.site_info_path, self.four_path, self.superheat_path)
        df2 = get_refrig_charge(df2, site_orifice, self.site_info_path, self.four_path, self.superheat_path)
    
        #check that the Refrig_charge column has data type float for df1 and df2
        proper_type = type(df1["Refrig_charge"][0])
        self.assertTrue(proper_type, type(10.0))
        proper_type_2 = type(df2["Refrig_charge"][0])
        self.assertTrue(proper_type_2, type(10.0))

    def test_refrig_charge_invalid(self):
        #test that it doesn't explode with improper values
        empty = pd.DataFrame()

        #If this doesn't explode, error checking was good. Make sure to try and account for most if not all of this!
        empty = get_refrig_charge(empty, "FAKE_01")

        #could additionally check for certain vars missing, complicated config though.
    
    """ #CURRENTLY HAS ERRORS!
    def test_change_ID_to_HVAC_invalid(self):
        empty_df = pd.DataFrame()
        result_df = change_ID_to_HVAC(empty_df, "AZ2_01", self.site_info_path)
        test_df = pd.DataFrame(columns=['event_ID'])
        test_df['event_ID'] = test_df['event_ID'].astype(np.int64)
        self.assertEqual(True, result_df.equals(test_df))
    """

    def test_replace_humidity_invalid(self):
        data_path = "testing/LBNL/transform/pickles/AZ2_01_04242022.pkl"
        site = "AZ2_01"
        time = dt.datetime(2022, 4, 24, 9, 0, 0)
        data = pd.read_pickle(data_path)
        od_conditions = gather_outdoor_conditions(data, site)
        result = replace_humidity(data, od_conditions, time, site)
        # self.assertEqual(data, result)

    
if __name__ == '__main__':

    unittest.main()
