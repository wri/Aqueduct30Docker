
# coding: utf-8

# In[6]:

""" Demand data is provided as volumes. Calculate Fluxes.
-------------------------------------------------------------------------------
PCRGLOBWB Data for demand is provided in different units: 
volumes (millionm3), flux (m or m3second), dimensionless. Converting to 
millionm3 (millionm3 per pixel per unit time) and m (m per pixel per unit time) 

Converts demand to flux
Converts supply and discharge to volume and flux

Author: Rutger Hofste
Date: 20180418
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    
    GLOBAL_CELLSIZE_M2_5MIN_ASSET_ID (string): Asset ID for earthengine
      image used to convert volumes to fluxes. 


Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2018M04D18_RH_Standardize_Demand_Units_5min_EE_V01"
EE_VERSION = 9
OUTPUT_VERSION = 1 

GLOBAL_CELLSIZE_M2_5MIN_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/global_cellsize_m2_05min"


ee_path = "projects/WRI-Aquaduct/PCRGlobWB20V{:02.0f}".format(EE_VERSION)


print("Input ee: " + ee_path +
      "\nOutput ee: " + ee_path )



# In[ ]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[1]:

import ee

ee.Initialize()


# In[2]:

# functions

def volume_to_flux_5min_millionm3_m2(image):
    
    
    


# In[9]:

sectors = ["PDom","PInd","PIrr","PLiv"]
demand_types = ["WW","WN"]
temporal_resolutions = ["year","month"]

for sector in sectors:
    for demand_type in demand_types:
        for temporal_resolution in temporal_resolutions:
            ic_file_name = "global_historical_{}{}_{}_millionm3_5min_1960_2014".format(sector,demand_type,temporal_resolution)
            ic_asset_id = "{}/{}".format(ee_path,ic_file_name)
            ic = ee.ImageCollection(ic_asset_id)
            



# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:

Previous runs:  

