
# coding: utf-8

# In[1]:

""" Demand data is provided as volumes. Calculate Fluxes.
-------------------------------------------------------------------------------
PCRGLOBWB Data for demand is provided in volumes with units millionm3 
(implicit per pixel/per time step); converting to fluxes (m) and storing to 
earth engine asset. 

Converts demand to flux
Converts supply and discharge to volume and flux

Author: Rutger Hofste
Date: 20180418
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

SCRIPT_NAME = "Y2018M04D18_RH_Demand_Fluxes_5min_EE_V01"
EE_VERSION = 9

OUTPUT_VERSION = 2 

TESTING = 0

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
import aqueduct3

ee.Initialize()


# In[4]:


def update_property_script_used(image):
    image = image.set("script_used",SCRIPT_NAME)
    return image

def update_property_output_version(image):
    image = image.set("output_version",OUTPUT_VERSION)
    return image

def get_df_from_ic(ic_input_asset_id):
    """Create a pandas dataframe with the image asset ids from an imagecollection.
    
    
    """
    command = "earthengine ls {}".format(ic_input_asset_id)
    asset_list_bytes = subprocess.check_output(command,shell=True).splitlines()
    asset_list =[x.decode("utf-8")  for x in asset_list_bytes]
    
    df = pd.DataFrame(asset_list)
    df.columns = ["input_image_asset_id"]
    df["input_ic_asset_id"] = ic_input_asset_id
    return df

    
def add_export_parameters_for_export(df,old_unit,new_unit):
    df_out = df.copy()
    df_out["output_ic_asset_id"] = df_out["input_ic_asset_id"].apply(lambda x: re.sub(old_unit,new_unit,x))
    df_out["output_image_asset_id"] = df_out["input_image_asset_id"].apply(lambda x: re.sub(old_unit,new_unit,x))
    df_out["description"] = df_out["output_image_asset_id"].apply(lambda x: (re.split("/",x)[-1])+"_V{:02.0f}".format(OUTPUT_VERSION)) 
    return df_out


def create_image_collections(df):
    """ Create ImageCollections based on export ic in dataframe
    
    finds unique output_ic_asset_id and creates imageCollections.
    
    """
    
    ic_asset_ids = list(df["output_ic_asset_id"].unique())
    
    
    for ic_asset_id in ic_asset_ids:
        command, result = aqueduct3.earthengine.create_imageCollection(ic_asset_id)
        print(command,result)


# In[ ]:

old_unit = "millionm3"
new_unit = "m"

sectors = ["PDom","PInd","PIrr","PLiv"]
demand_types = ["WW","WN"]
temporal_resolutions = ["year","month"]

if TESTING:
    sectors = ["PDom"]
    demand_types = ["WW"]
    temporal_resolutions = ["year"]


df = pd.DataFrame()
df_merged = pd.DataFrame()
for sector in sectors:
    for demand_type in demand_types:
        for temporal_resolution in temporal_resolutions:
            print(sector,demand_type,temporal_resolution)
            ic_input_file_name = "global_historical_{}{}_{}_millionm3_5min_1960_2014".format(sector,demand_type,temporal_resolution)
            ic_input_asset_id = "{}/{}".format(ee_path,ic_input_file_name)
            df = get_df_from_ic(ic_input_asset_id)
            df = add_export_parameters_for_export(df,old_unit,new_unit)
            create_image_collections(df)
            
            for index, row in df.iterrows():
                i_volume_millionm3_5min = ee.Image(row["input_image_asset_id"])
                i_flux_m_5min = aqueduct3.earthengine.volume_to_flux_5min_millionm3_m2(i_volume_millionm3_5min)
                i_flux_m_5min = update_property_script_used(i_flux_m_5min)
                i_flux_m_5min = update_property_output_version(i_flux_m_5min)
                
                description = row["description"]
                output_image_asset_id = row["output_image_asset_id"]
                aqueduct3.earthengine.export_image_global_5min(i_flux_m_5min,description,output_image_asset_id)
                print(output_image_asset_id)


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 
