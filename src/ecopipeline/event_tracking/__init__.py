"""
Event tracking and alarm detection utilities for the ecopipeline package.

This module exposes all public alarm-detection functions and alarm classes
used to identify fault conditions in heat-pump water-heater (HPWH) and
associated building-energy systems.  Functions accept post-transformed
minute and/or daily ``pd.DataFrame`` objects together with a
``ConfigManager`` instance and return a ``pd.DataFrame`` of detected alarm
events.

Alarm classes (e.g. ``Boundary``, ``ShortCycle``, ``AbnormalCOP``) can also
be imported directly from this module for lower-level use.
"""
from .event_tracking import *
from .Alarm import Alarm
from .alarms.ShortCycle import ShortCycle
from .alarms.TempRange import TempRange
from .alarms.LSInconsist import LSInconsist
from .alarms.SOOChange import SOOChange
from .alarms.BlownFuse import BlownFuse
from .alarms.HPWHOutage import HPWHOutage
from .alarms.BackupUse import BackupUse
from .alarms.HPWHOutlet import HPWHOutlet
from .alarms.HPWHInlet import HPWHInlet
from .alarms.BalancingValve import BalancingValve
from .alarms.TMSetpoint import TMSetpoint
from .alarms.AbnormalCOP import AbnormalCOP
from .alarms.PowerRatio import PowerRatio
from .alarms.Boundary import Boundary

__all__ = ['central_alarm_df_creator','flag_boundary_alarms','power_ratio_alarm','flag_abnormal_COP','flag_high_tm_setpoint',
           'flag_recirc_balance_valve','flag_hp_inlet_temp','flag_backup_use','flag_blown_fuse','flag_unexpected_soo_change','flag_shortcycle',
           'flag_hp_outlet_temp','flag_HP_outage','flag_unexpected_temp','flag_ls_mode_inconsistancy','Alarm','ShortCycle','TempRange','LSInconsist',
           'SOOChange','BlownFuse','HPWHOutage','BackupUse','HPWHOutlet','HPWHInlet','BalancingValve','TMSetpoint','AbnormalCOP','PowerRatio','Boundary']