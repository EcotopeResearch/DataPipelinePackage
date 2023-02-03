# Sets configurations
# TODO This doesn't work

_input_directory = "input/"
_output_directory = "output/"
_data_directory = "data/"
_config_directory = "input/config.ini"

def set_input(input: str):
    """
    Accessor function to set input directory in the format {directory}/
    Defaults to input/
    Input: String of relative directory
    """
    global _input_directory
    _input_directory = input
    return _input_directory


def set_output(output: str):
    """
    Accessor function to set output directory in the format {directory}/
    Defaults to output/
    Input: String of relative directory
    """
    global _output_directory
    _output_directory = output
    return _output_directory


def set_data(data: str):
    """
    Accessor function to set data directory in the format {directory}/
    Defaults to data/
    Input: String of relative directory
    """
    global _data_directory
    _data_directory = data
    return _data_directory

def set_config(cfg: str):
    """
    Accessor function to set input directory in the format {directory}/
    Defaults to input/
    Input: String of relative directory
    """
    global _config_directory
    _config_directory = cfg
    return _config_directory