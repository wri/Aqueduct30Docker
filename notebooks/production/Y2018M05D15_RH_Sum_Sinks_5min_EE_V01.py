
# coding: utf-8

# In[2]:

""" Calculate sum of sinks at 5min zones.
-------------------------------------------------------------------------------

If a sub-basin contains one or more sinks (coastal and endorheic), the sum 
of riverdischarge at those sinks will be used. If a subbasin does not contain
any sinks or is too small to be represented at 5min, the main channel 
riverdischarge (30s validfa_mask) will be used. 

Creates a table with 5min zones and sum of sinks. Export to pandas dataframe
and featurecollection. 

Args:

"""


TESTING = 0
SCRIPT_NAME = "Y2018M05D15_RH_Sum_Sinks_5min_EE_V01"
OUTPUT_VERSION = 1

ZONES5MIN_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_5min_V04"
LDD_EE_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/global_lddsound_numpad_05min"
ENDOSINKS_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2017M08D02_RH_Ingest_Aux_Rasters_GCS_EE_V02/output_V06/global_outletendorheicbasins_boolean_05min"

ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
print("Output ee: " +  ee_output_path)


# In[3]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

import ee
import aqueduct3
ee.Initialize()


# In[5]:

i_hybas_lev06_v1c_merged_fiona_5min = ee.Image(ZONES5MIN_EE_ASSET_ID)
i_ldd_5min = ee.Image(LDD_EE_ASSET_ID)
i_endosinks_5min = ee.Image(ENDOSINKS_EE_ASSET_ID)


# In[ ]:



