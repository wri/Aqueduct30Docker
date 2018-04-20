
# coding: utf-8

# In[4]:

""" Zonal statistics for basin area. Export in table format.
-------------------------------------------------------------------------------
Calculate the total area per basin in m2 for 5min and 30s resolution and for
level 00 and level 06 hydrobasins. Table is stored as pickle on S3 and postGIS.


Author: Rutger Hofste
Date: 20180420
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name


Returns:


"""

# Input Parameters
TESTING = 1
SCRIPT_NAME = "Y2018M04D20_RH_Zonal_Stats_Area_EE_V01"
EE_INPUT_ZONES_PATH = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V01"
EE_INPUT_VALUES_PATH = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02"
INPUT_VERSION_ZONES = 4
INPUT_VERSION_VALUES = 5
OUTPUT_VERSION = 1

gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ee zones: " +  EE_INPUT_ZONES_PATH +
      "\nInput ee values: " + EE_INPUT_VALUES_PATH +
      "\nOutput gcs: " + gcs_output_path)




# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import ee
import aqueduct3
ee.Initialize()





# In[5]:

geometry = aqueduct3.earthengine.get_global_geometry(TESTING)


# In[7]:

spatial_resolutions = ["5min","30s"]
pfaf_levels = ["06","00"]

for spatial_resolution in spatial_resolutions:
    for pfaf_level in pfaf_levels:
        print(spatial_resolution,pfaf_level)
        i_zones_asset_id = "{}/hybas_lev{}_v1c_merged_fiona_{}_V{:02.0f}".format(EE_INPUT_ZONES_PATH,pfaf_level,spatial_resolution,INPUT_VERSION_ZONES)
        i_values_asset_id = "{}/global_area_m2_{}_V{:02.0f}".format(EE_INPUT_VALUES_PATH,spatial_resolution,INPUT_VERSION_VALUES)
        total_image = ee.Image(i_values_asset_id).addBands(ee.Image(i_zones_asset_id))
        
        


# In[13]:

reducer = ee.Reducer.mean().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")


# In[14]:

test = total_image.reduceRegion(
        geometry = geometry,
        reducer= reducer,
        scale= 1000,
        maxPixels=1e10
      ).get("groups")


# In[15]:

test.getInfo()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:

Previous runs:  
0:24:15.930678    

