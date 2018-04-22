
# coding: utf-8

# In[1]:

""" Zonal statistics for basin demand. Export in table format.
-------------------------------------------------------------------------------


Demand data is provided in volumes (millionm3) at 5min resolution. 

Steps:
    - Convert to flux at 5min.
    - (under the hood) Convert to flux at 30s.
    - Zonal stats for flux at 30s.





Author: Rutger Hofste
Date: 20180422
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

# Input Parameters
TESTING = 1
SCRIPT_NAME = "Y2018M04D22_RH_Zonal_Stats_Demand_EE_V01"

EE_INPUT_ZONES_PATH = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V01"
EE_INPUT_VALUES_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V09"
INPUT_VERSION_ZONES = 4

OUTPUT_VERSION = 1

SEPARATOR = "_|-"
SCHEMA = ["geographic_range",
     "temporal_range",
     "indicator",
     "temporal_resolution",
     "unit",
     "spatial_resolution",
     "temporal_range_min",
     "temporal_range_max"]

# Output Parameters
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


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

# Imports
import pandas as pd
import ee
import aqueduct3
ee.Initialize()


# In[4]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[ ]:

def zonal_stats():
    """ Zonal Statistics for """
    
    
    


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[5]:

sectors = ["PDom","PInd","PIrr","PLiv"]
demand_types = ["WW","WN"]
temporal_resolutions = ["year","month"]

#spatial_resolutions = ["5min","30s"]
#pfaf_levels = [6,0]

spatial_resolutions = ["30s"]
pfaf_levels = [6]

if TESTING:
    sectors = ["PDom"]
    demand_types = ["WW"]
    temporal_resolutions = ["year"]
    

reducer_names = ["mean"]

for reducer_name in reducer_names:
    reducer = aqueduct3.earthengine.get_grouped_reducer(reducer_name)
    
    for spatial_resolution in spatial_resolutions:
        crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)

        for pfaf_level in pfaf_levels:
            
            for sector in sectors:
                for demand_type in demand_types:
                    for temporal_resolution in temporal_resolutions:
                        print(reducer_name,spatial_resolution,pfaf_level,sector,demand_type,temporal_resolution)
                        i_zones_asset_id = "{}/hybas_lev{:02.0f}_v1c_merged_fiona_{}_V{:02.0f}".format(EE_INPUT_ZONES_PATH,pfaf_level,spatial_resolution,INPUT_VERSION_ZONES)
                        ic_values_input_asset_id = "{}/global_historical_{}{}_{}_m_5min_1960_2014".format(EE_INPUT_VALUES_PATH,sector,demand_type,temporal_resolution)
                        df = aqueduct3.earthengine.get_df_from_ic(ic_values_input_asset_id)
                        
                        for index, row in df.iterrows():
                            i_values_input_asset_id = row["input_image_asset_id"]
                            
                                                        
                            dictje = aqueduct3.split_key(i_values_input_asset_id,SCHEMA,SEPARATOR)
                            """
                            i_values = ee.Image(i_values_input_asset_id)
                            
                            total_image = ee.Image(i_values_asset_id).addBands(ee.Image(i_zones_asset_id))
                            
                            result_list = total_image.reduceRegion(geometry = geometry,
                                            reducer= reducer,
                                            crsTransform = crs_transform,
                                            maxPixels=1e10
                                            ).get("groups")
                            """
                            
                        


# In[ ]:




# In[ ]:

df.loc[0]["input_image_asset_id"]


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:

Previous runs:  
0:24:15.930678    

