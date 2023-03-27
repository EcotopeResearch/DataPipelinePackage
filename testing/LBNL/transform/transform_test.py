import unittest
import pandas as pd
from ecotope_package_cs2306 import get_refrig_charge 

class Test_Transform(unittest.TestCase):
    #NOTE: If you want to run the tests w/an updated LBNL, you have to run the install script 
    #again w/pip, saved here: 
    #pip uninstall "PLACEHOLDER"
    #pip install "PLACEHOLDER"

    #class data (maybe, var_names directory)
    site_info_path = "LBNL-input/site_info.csv"
    four_path = "LBNL-input/410a_pt.csv"
    superheat_path = "LBNL-input/superheat.csv"

    #NOTE: Instead of checking if files are missing inside functions, we instead do that at the start? 
    #Check that test function instead of all of those

    #Carlos
    def test_create_fan_curves(self):
        pass

    #Casey
    def test_gas_valve_diff(self):
        pass

    #Casey 
    def test_gather_outdoor_conditions(self):
        pass

    #Julian 
    def test_refrig_charge_valid(self):
        #we assume proper input variables!
        
        #txv pickle
        txv = "pickles/AZ2_01_04202022.pkl"
        site_txv = "AZ2_01"
        df1 = pd.read_pickle(txv)
        #superheat pickle
        orifice = "pickles/IL2_01_06182022.pkl"
        site_orifice = " IL2_01"
        df2 = pd.read_pickle(orifice)

        #function calls!
        #NOTE: I still need to double check whether or not it needs to be returned? It does, try that!!
        df1 = get_refrig_charge(df1, site_txv, self.site_info_path, self.four_path, self.superheat_path)
        #df2 = get_refrig_charge(df2, site_orifice, self.site_info_path, self.four_path, self.superheat_path)

    
        #check that the Refrig_charge column has data for df1, and that it has "None" for df2! NOTE: Probably just df2 pending atm
        #Just check the first five elements, or something like that, make sure they have values. Should be a negative float in this case!
        proper_type = type(df1["Refrig_charge"])
        self.assertTrue(proper_type, type(10.0))
        pass

    def test_refrig_charge_invalid(self):
        #test that it doesn't explode with improper values
        empty = pd.DataFrame()

        #If this doesn't explode, error checking was good. Make sure to try and account for most if not all of this!
        empty = get_refrig_charge(empty, "FAKE_01", "fake_info.csv", "fake_four_path.csv", "fake_superheat_path.csv")

        pass

if __name__ == '__main__':
    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()