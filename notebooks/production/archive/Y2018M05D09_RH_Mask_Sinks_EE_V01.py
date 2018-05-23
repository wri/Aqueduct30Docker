
# coding: utf-8

# In[ ]:

""" Mask sinks that have already been used in the max flow accumumation 
scripts.
-------------------------------------------------------------------------------

The previous script (Y2018M05D04_RH_Zonal_Stats_Supply_EE_V01) calculted the 
available riverdischarge in the valid most downstream pixels (30s). There are
cases however where multiple streams need to be summed up to determine the 
total riverdischarge available. The image of sinks needs to be masked by the
pixels already used to avoid double counting. 




Author: Rutger Hofste
Date: 20180509
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    TESTING (boolean) : Testing mode. Uses a smaller geography if enabled.
    
    SCRIPT_NAME (string) : Script name.
    EE_INPUT_ZONES_PATH (string) : earthengine input path for zones.
    EE_INPUT_VALUES_PATH (string) : earthengine input path for value images.
    INPUT_VERSION_ZONES (integer) : input version for zones images.
    INPUT_VERSION_VALUES (integer) : input version for value images.
    OUTPUT_VERSION (integer) : output version. 
    EXTRA_PROPERTIES (dictionary) : Extra properties to store in the resulting
        pandas dataframe. 
    

Returns:

"""

TESTING = 0
SCRIPT_NAME = "Y2018M05D09_RH_Mask_Sinks_EE_V01"
OUTPUT_VERSION = 1


