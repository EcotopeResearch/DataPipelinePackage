# DataPipelinePackage

## To Install the Package

    $ pip install ecopipeline

## Functions in Data Pipeline package:
### extract.py
- load raw JSON-formatted data into a dataframe
- merge NOAA weather data with sensor data

### clean.py
- remove outliers specified in the configuration file
- send alert to Ecotope engineers about sensor malfunction
- remove any nan values

### unit_conv.py
- convert data into desired units

### derived_var.py
- calculate the COP (coefficient of preformance) values and other derived values
- aggregate data by day and week

### load.py
- load the data into Ecotope's database
