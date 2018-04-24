
# coding: utf-8

# In[1]:

"""Supply and discharge data is provided as m3second. Convert to millionm3.
-------------------------------------------------------------------------------
PCRGLOBWB Data for demand is provided in volumes with units m3second
(implicit per pixel/per time step); converting to volumes (millionm3) and 
storing to 
earth engine asset. 


Author: Rutger Hofste
Date: 20180419
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    EE_VERSION (integer) : Earth Engine path version.
    OUTPUT_VERSION (integer) : Output version.
    TESTING (boolean) : Testing.

Returns:

"""

# Input Parameters

SCRIPT_NAME = "Y2018M04D19_RH_Supply_Discharge_Volume_5min_EE_V01"
EE_VERSION = 9

OUTPUT_VERSION = 1 

TESTING = 0

SEPARATOR = "_|-"

SCHEMA = ["geographic_range",
     "temporal_range",
     "indicator",
     "temporal_resolution",
     "unit",
     "spatial_resolution",
     "temporal_range_min",
     "temporal_range_max"]

ee_path = "projects/WRI-Aquaduct/PCRGlobWB20V{:02.0f}".format(EE_VERSION)


print("Input ee: " + ee_path +
      "\nOutput ee: " + ee_path )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import subprocess
import pandas as pd
import ee
import re
from retrying import retry
from datetime import timedelta
import aqueduct3

ee.Initialize()


# In[4]:

def update_property_script_used(image):
    image = image.set("script_used",SCRIPT_NAME)
    return image

def update_property_output_version(image):
    image = image.set("output_version",OUTPUT_VERSION)
    return image


def ic_flux_to_volume_5min_m3second_millionm3(ic_input_asset_id,output_version,old_units,new_units,schema,separator):
    """ Convert an imagecollection from flux to volume.
    -------------------------------------------------------------------------------
    The result is stored in an imagecollection with the same name as the input
    imagecollection but with 'millionm3' instead of 'm3second'
    
    Input ic:
    global_historical_riverdischarge_month_m3second_5min_1960_2014
    
    Output ic:    
    global_historical_riverdischarge_month_millionm3_5min_1960_2014
    
    Args:
        ic_input_asset_id (string) : asset id of input imagecollection.
        output_version (integer) : output version. 
        old_units (string) : Old units.
        new_units (string) : New units. 
        schema (list) : A list of strings containing the schema. See 
            aqueduct3.split_key() for more info.
        separator (regex) : Regular expression of separators used in geotiff
            filenames.    

    """
    start_time = time.time()
    df_errors = pd.DataFrame()
    
    df = aqueduct3.earthengine.get_df_from_ic(ic_input_asset_id)
    df = aqueduct3.earthengine.add_export_parameters_for_export(df,old_unit,new_unit,output_version)
    
    # Creating ImageCollection(s)
    output_ic_asset_ids = list(df["output_ic_asset_id"].unique())
    for output_ic_asset_id in output_ic_asset_ids:
        command, result = aqueduct3.earthengine.create_imageCollection(output_ic_asset_id)
        print(command,result)
    
    # Bacth Converting and uploading.     
    for index, row in df.iterrows():
        elapsed_time = time.time() - start_time 
        print("Index: {:04.0f} Elapsed: {}".format(index, timedelta(seconds=elapsed_time)))
    
        description = row["description"]
        output_image_asset_id = row["output_image_asset_id"]
        
        # get additional parameters from asset name
        # add fictional extension to 
        key = row["input_image_asset_id"] + ".ee_image"
        dictje = aqueduct3.split_key(key,schema,separator)
        
        
    
        if aqueduct3.earthengine.asset_exists(output_image_asset_id):
            print("Asset exists, skipping: {}".format(output_image_asset_id))
        else:
            i_old_unit_5min = ee.Image(row["input_image_asset_id"])                
            if old_unit == "m3second" and new_unit == "millionm3":
                year = int(dictje["year"])
                month = int(dictje["month"])
                temporal_resolution = dictje["temporal_resolution"]
                
                i_new_unit_5min = aqueduct3.earthengine.flux_to_volume_5min_m3second_millionm3(i_old_unit_5min,temporal_resolution,year,month) 
            else:
                raise("Error: invalid combination of units.") 
            i_new_unit_5min = update_property_script_used(i_new_unit_5min)
            i_new_unit_5min = update_property_output_version(i_new_unit_5min)
            

            aqueduct3.earthengine.export_image_global_5min(i_new_unit_5min,description,output_image_asset_id)
            print(output_image_asset_id)  
         

    return i_new_unit_5min


# In[5]:

old_unit = "m3second"
new_unit = "millionm3"

temporal_resolutions = ["year","month"]
indicators = ["riverdischarge"]


# In[6]:

df = pd.DataFrame()
for indicator in indicators:
    for temporal_resolution in temporal_resolutions:
        ic_input_file_name = "global_historical_{}_{}_m3second_5min_1960_2014".format(indicator,temporal_resolution)
        ic_input_asset_id = "{}/{}".format(ee_path,ic_input_file_name)
        df = ic_flux_to_volume_5min_m3second_millionm3(ic_input_asset_id,OUTPUT_VERSION,"m3second","millionm3",SCHEMA,SEPARATOR)


# In[7]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:43:08.519624

# In[ ]:



