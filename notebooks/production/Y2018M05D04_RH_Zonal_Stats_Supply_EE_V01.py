
# coding: utf-8

# In[1]:

""" Zonal statistics for basin demand. Export in table format.
-------------------------------------------------------------------------------
Zonal statistics for basin area. Export in table format.

Strategy:

1. first riverdischarge in zones masked by previous script (max_fa)

2. mask endorheic basins with mask from previous script

3. sum riverdischarge in remaining pixels




Author: Rutger Hofste
Date: 20180504
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

TESTING = 1
SCRIPT_NAME = "Y2018M05D04_RH_Zonal_Stats_Supply_EE_V01"

EE_INPUT_ZONES_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_30s_V04"
EE_INPUT_RIVERDISCHARGE_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_riverdischarge_month_millionm3_5min_1960_2014"
EE_INPUT_FA_ASSET_ID = "projects/WRI-Aquaduct/Y2017M08D02_RH_Ingest_Aux_Rasters_GCS_EE_V02/output_V06/global_accumulateddrainagearea_km2_05min"
EE_INPUT_MAX_MASKEDFA_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D03_RH_Max_FA_Add_Sinks_EE_V01/output_V02/global_max_maskedaccumulateddrainagearea_km2_30sPfaf06"



# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# Imports
import pandas as pd
from datetime import timedelta
import os
import ee
import aqueduct3

ee.Initialize()


# In[4]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/{}.log".format(SCRIPT_NAME))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[5]:

def master(i_zones,i_values,geometry,crs_transform,statistic_type,extra_properties):
    result_list = aqueduct3.earthengine.raster_zonal_stats(
                                            i_zones = i_zones,
                                            i_values = i_values,
                                            statistic_type = statistic_type,
                                            geometry = geometry_server_side,
                                            crs_transform = crs_transform,
                                            crs="EPSG:4326")
    i_result, i_count = aqueduct3.earthengine.zonal_stats_results_to_image(result_list,i_zones,statistic_type)
    
    i_dummy_result_properties = aqueduct3.earthengine.zonal_stats_image_propertes(i_zones,i_values,extra_properties,zones_prefix="zones_",values_prefix="values_")
    
    i_result = i_result.multiply(1) #Deletes old properties
    i_result = i_result.copyProperties(i_dummy_result_properties)
    
    return i_result, i_count


# In[6]:

# Images

i_zones_30sPfaf06 = ee.Image(EE_INPUT_ZONES_ASSET_ID)
ic_riverdischarge_month_millionm3_05min = ee.ImageCollection(EE_INPUT_RIVERDISCHARGE_ASSET_ID)
i_fa_km2_05min = ee.Image(EE_INPUT_FA_ASSET_ID)
i_max_maskedfa_km2_30sPfaf06 = ee.Image(EE_INPUT_MAX_MASKEDFA_ASSET_ID)


# Geospatial constants
spatial_resolution = "30s"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)


# In[8]:




# In[9]:




# In[10]:

# 0. Mask from previous script: (Y2018M05D03_RH_Max_FA_Add_Sinks_EE_V01 == flow accumulation ) 
i_mask_max_maskedfa_boolean_30sPfaf06 =  i_max_maskedfa_km2_30sPfaf06.eq(i_fa_km2_05min)
i_masked_zones_30sPfaf06 = i_zones_30sPfaf06.multiply(i_mask_max_maskedfa_boolean_30sPfaf06)

# 1. first riverdischarge in zones masked by previous script (max_fa)

i_riverdischarge_month_millionm3_05min = ee.Image(ic_riverdischarge_month_millionm3_05min.first())

output_dict = {}
output_dict["i_first_riverdischarge_month_millionm3_05min"], output_dict["count_riverdischarge_month_millionm3_05min"] = master(i_zones = i_hybas_lev06_v1c_merged_fiona_30s_V04,
                                                                                                                                                   i_values = i_global_accumulateddrainagearea_km2_05min_masked,
                                                                                                                                                   geometry = geometry_client_side,
                                                                                                                                                   crs_transform = crs_transform,
                                                                                                                                                   statistic_type = "first",
                                                                                                                                                   extra_properties= {})



# In[ ]:



