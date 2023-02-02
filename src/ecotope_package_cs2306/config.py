# Sets configurations
# TODO This doesn't work
def set_input(input: str = "input/"):
    """
    Accessor function to set input directory in the format {directory}/
    Defaults to input/
    Input: String of relative directory
    """
    _input_directory = input
    return _input_directory


def set_output(output: str = "output/"):
    """
    Accessor function to set output directory in the format {directory}/
    Defaults to output/
    Input: String of relative directory
    """
    _output_directory = output
    return _output_directory


def set_data(data: str = "data/"):
    """
    Accessor function to set data directory in the format {directory}/
    Defaults to data/
    Input: String of relative directory
    """
    _data_directory = data
    return _data_directory

def set_config(cfg: str = "input/config.ini"):
    """
    Accessor function to set input directory in the format {directory}/
    Defaults to input/
    Input: String of relative directory
    """
    _config_directory = cfg
    return _config_directory


global _input_directory
global _output_directory
global _data_directory
global _config_directory

_input_directory = set_input()
_output_directory = set_output()
_data_directory = set_data()
_config_directory = set_config()