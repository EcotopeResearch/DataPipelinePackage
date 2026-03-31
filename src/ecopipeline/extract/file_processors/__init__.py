"""File-processor classes for the ecopipeline extract layer.

Each class in this sub-package extends
:class:`~ecopipeline.extract.FileProcessor.FileProcessor` and provides
format-specific parsing logic for a particular family of raw data files.

Classes
-------
CSVProcessor
    Generic CSV files with a named string timestamp column.
JSONProcessor
    JSON-formatted raw data files.
ModbusCSVProcessor
    Modbus / Acquisuite CSV files whose filenames encode a hexadecimal UTC
    timestamp.
DentCSVProcessor
    DENT power-meter CSV files with 12-row headers and separate date/time
    columns.
FlowCSVProcessor
    Flow-meter CSV files with 6-row headers and split Year/Month/Day/Hour/
    Minute/Second columns.
MSACSVProcessor
    MSA CSV files with a ``DateEpoch(secs)`` Unix-epoch timestamp column.
EGaugeCSVProcessor
    eGauge CSV files with a ``Date & Time`` Unix-epoch timestamp column;
    cumulative register values are differenced into interval deltas.
SmallPlanetCSVProcessor
    Small Planet Controls CSV files; column names are remapped through
    ``Variable_Names.csv`` and unmapped columns are dropped.
"""

from .CSVProcessor import CSVProcessor
from .JSONProcessor import JSONProcessor
from .ModbusCSVProcessor import ModbusCSVProcessor
from .DentCSVProcessor import DentCSVProcessor
from .FlowCSVProcessor import FlowCSVProcessor
from .MSACSVProcessor import MSACSVProcessor
from .EGaugeCSVProcessor import EGaugeCSVProcessor
from .SmallPlanetCSVProcessor import SmallPlanetCSVProcessor
