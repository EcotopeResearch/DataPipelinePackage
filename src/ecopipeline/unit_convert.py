from datetime import datetime
import pandas as pd
import numpy as np
# Format for unit Conversions
# {value type}_{from unit}_to_{to unit}({value type}_{from unit})
# Ex. temp_c_to_f(temp_c)

# Used in NOAA Data
def temp_c_to_f(temp_c : float):
    temp_f = 32 + (temp_c/10)* 9/5
    return temp_f

# Used in NOAA Data
def divide_num_by_ten(num : float):
    return (num/10)

# Used in NOAA Data
def windspeed_mps_to_knots(speed : float):
    speed_kts = 1.9438445 * speed/10
    return speed_kts

# Used in NOAA Data
def precip_cm_to_mm(precip : float):
    precip_mm = 0
    if precip == -1:
        precip_mm = -1
    else:
        precip_mm = divide_num_by_ten(precip)
    return precip_mm

# Used in NOAA Data
def conditions_index_to_desc(conditions: int):
    conditions_desc = 0 
    match conditions:
        case 0: 
            conditions_desc = 'None, SKC or CLR'
        case 1: 
            conditions_desc = 'One okta - 1/10 or less but not zero'
        case 2: 
            conditions_desc = 'Two oktas - 2/10 - 3/10, or FEW'
        case 3: 
            conditions_desc = 'Three oktas - 4/10'
        case 4: 
            conditions_desc = 'Four oktas - 5/10, or SCT'
        case 5: 
            conditions_desc = 'Five oktas - 6/10'
        case 6: 
            conditions_desc = 'Six oktas - 7/10 - 8/10'
        case 7: 
            conditions_desc = 'Seven oktas - 9/10 or more but not 10/10, or BKN'
        case 8: 
            conditions_desc = 'Eight oktas - 10/10, or OVC'
        case 9: 
            conditions_desc = 'Sky obscured, or cloud amount cannot be estimated'
        case 10: 
            conditions_desc = 'Partial obscuration'
        case 11: 
            conditions_desc = 'Thin scattered'
        case 12: 
            conditions_desc = 'Scattered'
        case 13: 
            conditions_desc = 'Dark scattered'
        case 14: 
            conditions_desc = 'Thin broken'
        case 15: 
            conditions_desc = 'Broken'
        case 16: 
            conditions_desc = 'Dark broken'
        case 17: 
            conditions_desc = 'Thin overcast'
        case 18: 
            conditions_desc = 'Overcast'
        case 19: 
            conditions_desc = 'Dark overcast'  
        case _:
            conditions_desc = np.NaN  
    return conditions_desc

# Used in verify_power_energy() in transform
def energy_to_power(energy : float):
    return energy * 60

# Used in aggregate_values
def energy_btu_to_kwh(sensor_readings):
    return sensor_readings / (60 * 3.412)

def energy_kwh_to_kbtu(gpm, delta_t):
    return 60 * 8.33 * gpm * delta_t / 1000

def power_flow_to_kW(gpm, delta_t):
    return 60 * 8.33 * gpm * delta_t / 3412