
# Format for unit Conversions
# {value type}_{from unit}_to_{to unit}({value type}_{from unit})
# Ex. temp_c_to_f(temp_c)

def temp_c_to_f(temp_c : float):
    temp_f = 32 + (temp_c/10)* 9/5
    return temp_f
