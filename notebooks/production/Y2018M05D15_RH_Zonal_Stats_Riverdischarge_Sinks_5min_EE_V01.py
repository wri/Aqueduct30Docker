
# coding: utf-8

# In[6]:

""" Sum riverdischarge at sinks at 5min resolution. 
-------------------------------------------------------------------------------

If a sub-basin contains one or more sinks (coastal and endorheic), the sum 
of riverdischarge at those sinks will be used. If a subbasin does not contain
any sinks or is too small to be represented at 5min, the main channel 
riverdischarge (30s validfa_mask) will be used. 

takes sum of riverdischarge at 5min sinks per zone. 


Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

TESTING = 1
SCRIPT_NAME = "Y2018M05D15_RH_Zonal_Stats_Riverdischarge_Sinks_5min_EE_V01"
OUTPUT_VERSION = 1

EXTRA_PROPERTIES = {"nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}

ZONES5MIN_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_5min_V04"
LDD_EE_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/global_lddsound_numpad_05min"
Q_EE_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_riverdischarge_month_millionm3_5min_1960_2014"

ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Output ee: " +  ee_output_path,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path)


# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

import pandas as pd
import numpy as np
import ee
import aqueduct3
ee.Initialize()


# In[7]:

i_hybas_lev06_v1c_merged_fiona_5min = ee.Image(ZONES5MIN_EE_ASSET_ID)
i_ldd_5min = ee.Image(LDD_EE_ASSET_ID)


# In[8]:

i_sinks_5min =  i_ldd_5min.eq(5)
i_sinks_5min = i_sinks_5min.copyProperties(i_ldd_5min)
i_sinks_5min = i_sinks_5min.set("unit","boolean")


# In[9]:




# In[10]:

# Geospatial constants
spatial_resolution = "5min"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)


# In[11]:

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
    
    return result_list,i_result, i_count


# In[ ]:



