import unittest
import pandas as pd
from ecotope_package_cs2306 import get_refrig_charge 

class Test_Transform(unittest.TestCase):
    #class data (maybe, var_names directory)

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
    def test_fridge_placeholder_valid(self):
        #test that the files you need have the proper variables

        #test that Refrig_charge gets modified in the dataframe
        pass

if __name__ == '__main__':
    #runs test_xxx functions, shows what passed or failed. 
    unittest.main()