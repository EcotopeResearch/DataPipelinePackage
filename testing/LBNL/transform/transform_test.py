import unittest
import pandas as pd
from ecotope_package_cs2306 import get_refrig_charge, gas_valve_diff

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
    def test_create_fan_curves(self):
        pass

    #Casey
    def test_gas_valve_diff_valid(self):
        # site AZ2_01 uses hp (not gas) heating
        hp_pickle = "testing/LBNL/transform/pickles/AZ2_01_04202022.pkl"
        hp_df = pd.read_pickle(hp_pickle)
        result_hp_df = gas_valve_diff(hp_df, "AZ2_01", self.site_info_path)

        # site IL2_01 uses gas heating 
        gas_pickle = "testing/LBNL/transform/pickles/IL2_01_06182022.pkl"
        gas_df = pd.read_pickle(gas_pickle)
        result_gas_df = gas_valve_diff(gas_df, "IL2_01", self.site_info_path)
        
        #self.assertNotEqual(gas_df.iloc[:, 1].sum(), result_gas_df.iloc[:, 1].sum())
        self.assertEqual(hp_df.iloc[:, 1].sum(), result_hp_df.iloc[:, 1].sum())
        
    
    def test_gas_valve_diff_invalid(self):
        empty_df = pd.DataFrame()
        result_df = gas_valve_diff(empty_df, "AZ2_01", self.site_info_path)
        self.assertEqual(True, empty_df.equals(result_df))

    #Casey 
    def test_gather_outdoor_conditions(self):
        pass

    #Julian 
    def test_refrig_charge_valid(self):
        #we assume proper input variables!
        
        #txv pickle
        txv = "testing/LBNL/transform/pickles/AZ2_01_04202022.pkl"
        site_txv = "AZ2_01"
        df1 = pd.read_pickle(txv)
        #superheat pickle
        orifice = "testing/LBNL/transform/pickles/IL2_01_06182022.pkl"
        site_orifice = " IL2_01"
        df2 = pd.read_pickle(orifice)

        #function calls!
        #NOTE: We need a pickle for df2! Waiting on completion of other functions/order
        df1 = get_refrig_charge(df1, site_txv, self.site_info_path, self.four_path, self.superheat_path)
        #df2 = get_refrig_charge(df2, site_orifice, self.site_info_path, self.four_path, self.superheat_path)
    
        #check that the Refrig_charge column has data for df1, and that it has "None" for df2! NOTE: Probably just df2 pending atm
        #Just check the first five elements, or something like that, make sure they have values. Should be a negative float in this case!
        proper_type = type(df1["Refrig_charge"]) #TODO: THIS NEEDS TO CHECK FIRST FEW ELEMENTS, NOT HOW IT'S DONE HERE!!
        self.assertTrue(proper_type, type(10.0))
        pass

    def test_refrig_charge_invalid(self):
        #test that it doesn't explode with improper values
        empty = pd.DataFrame()

        #If this doesn't explode, error checking was good. Make sure to try and account for most if not all of this!
        empty = get_refrig_charge(empty, "FAKE_01", "fake_info.csv", "fake_four_path.csv", "fake_superheat_path.csv")

if __name__ == '__main__':
    #runs test_xxx functions, shows what passed or failed. 
    
    """
    #pure testing grabs
    site_info_path = "testing/LBNL/transform/LBNL-input/site_info.csv"
    four_path = "testing/LBNL/transform/LBNL-input/410a_pt.csv"
    superheat_path = "testing/LBNL/transform/LBNL-input/superheat.csv"
    orifice_path = "testing/LBNL/transform/pickles/IL2_01_06182022.pkl"

    #or_output_testing
    or_pickle = pd.read_pickle(orifice_path)
    df = pd.DataFrame

    #NOTE: To do proper testing, I need a pickle with all the stuff that happens before superheat!
    print(df)
    """
    
    unittest.main()