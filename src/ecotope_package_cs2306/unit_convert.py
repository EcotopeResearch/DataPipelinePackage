
# Format for unit Conversions
# {value type}_{from unit}_to_{to unit}({value type}_{from unit})
# Ex. temp_c_to_f(temp_c)

def temp_c_to_f(temp_c : float):
    temp_f = 32 + (temp_c/10)* 9/5
    return temp_f

def divide_num_by_ten(num : float):
    return (num/10)

def windspeed_mps_to_knots(speed : float):
    speed_kts = 1.9438445 * speed/10
    return speed_kts

def precip_cm_to_mm(precip : float):
    precip_mm = 0
    if precip == -1:
        precip_mm = -1
    else:
        precip_mm = divide_num_by_ten(precip)
    return precip_mm

def winddirection_index_to_deg(wind_direction: int):
    wind_direction_deg = 0 
    match wind_direction:
        case 0: 
            wind_direction_deg = 'None, SKC or CLR'
        case 1: 
            wind_direction_deg = 'One okta - 1/10 or less but not zero'
        case 2: 
            wind_direction_deg = 'Two oktas - 2/10 - 3/10, or FEW'
        case 3: 
            wind_direction_deg = 'Three oktas - 4/10'
        case 4: 
            wind_direction_deg = 'Four oktas - 5/10, or SCT'
        case 5: 
            wind_direction_deg = 'Five oktas - 6/10'
        case 6: 
            wind_direction_deg = 'Six oktas - 7/10 - 8/10'
        case 7: 
            wind_direction_deg = 'Seven oktas - 9/10 or more but not 10/10, or BKN'
        case 8: 
            wind_direction_deg = 'Eight oktas - 10/10, or OVC'
        case 9: 
            wind_direction_deg = 'Sky obscured, or cloud amount cannot be estimated'
        case 10: 
            wind_direction_deg = 'Partial obscuration'
        case 11: 
            wind_direction_deg = 'Thin scattered'
        case 12: 
            wind_direction_deg = 'Scattered'
        case 13: 
            wind_direction_deg = 'Dark scattered'
        case 14: 
            wind_direction_deg = 'Thin broken'
        case 15: 
            wind_direction_deg = 'Broken'
        case 16: 
            wind_direction_deg = 'Dark broken'
        case 17: 
            wind_direction_deg = 'Thin overcast'
        case 18: 
            wind_direction_deg = 'Overcast'
        case 19: 
            wind_direction_deg = 'Dark overcast'    
    return wind_direction_deg