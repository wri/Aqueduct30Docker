
# coding: utf-8

# In[1]:

""" Zonal statistics for basin demand. Export in table format.
-------------------------------------------------------------------------------
Demand data is provided in volumes (millionm3) at 5min resolution. 
For further analysis the 30s rasterized zones of hydrobasin level 6 will be 
used.

Both volumetric and flux demand data is useful. Riverdischarge is calculated
in volumes. For demand data, fluxes are being used. Water stress will be
calculated by dividing the fluxes of demand and available supply. 

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
TESTING = 0
SCRIPT_NAME = "Y2018M04D22_RH_Zonal_Stats_Demand_EE_V01"

EE_INPUT_ZONES_PATH = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V01"
EE_INPUT_VALUES_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V09"
INPUT_VERSION_ZONES = 4

OUTPUT_VERSION = 1

SPATIAL_RESOLUTIONS = ["30s"]
PFAF_LEVELS = [6]

SECTORS = ["PDom","PInd","PIrr","PLiv"]
DEMAND_TYPES = ["WW","WN"]
TEMPORAL_RESOLUTIONS = ["year","month"]
REDUCER_NAMES = ["mean"]

SEPARATOR = "_|-"
SCHEMA =["geographic_range",
         "temporal_range",
         "indicator",
         "temporal_resolution",
         "unit",
         "spatial_resolution",
         "temporal_range_min",
         "temporal_range_max"]

EXTRA_PROPERTIES = {"output_version":OUTPUT_VERSION,
                    "script_used":SCRIPT_NAME,
                   }


# Output Parameters
gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input ee zones: " +  EE_INPUT_ZONES_PATH +
      "\nInput ee values: " + EE_INPUT_VALUES_PATH +
      "\nOutput gcs: " + gcs_output_path)


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

#!rm -r {ec2_output_path}
#!mkdir -p {ec2_output_path}


# In[8]:

def post_process_results(result_list,function_properties,extra_properties=EXTRA_PROPERTIES):
    """Client side function to convert results of reduceRegion to pandas dataframe.
    -------------------------------------------------------------------------------
    
    Adds additional properties. The script is client side for convenience reasons.
    A more robust and fast approach would be to add the extra_properties to the 
    server side dictionary.
    
    Args:
        result_list (ee.List) : List of dictionaries. Result from reduceRegion
        function_properties (dictionary) : Additional properties used in the 
            reduceRegion function call.
        extra_properties (dictionary) : Additional properties set at global level. 
    
    Returns:
        df (pd.DataFrame) : Pandas dataframe with extra properties.
    
    
    """
    extra_properties = {**function_properties, **EXTRA_PROPERTIES}
    result_list_clientside = result_list.getInfo()
    df = pd.DataFrame(result_list_clientside)
    df = df.assign(**extra_properties)
    df = df.apply(pd.to_numeric, errors='ignore')
    return df  


def main():
    geometry = aqueduct3.earthengine.get_global_geometry(TESTING)
    i_processed = 0

    if TESTING:
        sectors = ["PDom"]
        demand_types = ["WW"]
        temporal_resolutions = ["year"]
        reducer_names = ["mean"]
        spatial_resolutions = ["30s"]
        pfaf_levels = [6]
    else:
        sectors = SECTORS
        demand_types = DEMAND_TYPES
        temporal_resolutions = TEMPORAL_RESOLUTIONS
        reducer_names = REDUCER_NAMES
        spatial_resolutions = SPATIAL_RESOLUTIONS
        pfaf_levels = PFAF_LEVELS

    start_time = time.time()
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

                            if TESTING:
                                df = df[1:3]
                            else:
                                pass

                            for index, row in df.iterrows():
                                i_processed = i_processed + 1
                                elapsed_time = time.time() - start_time
                                i_values_input_asset_id = row["input_image_asset_id"]
                                # Add an artificial extension to allow the function to run. 
                                # consider updating the split_key function to handle cases without an extension.
                                i_values_input_asset_id_extenstion = i_values_input_asset_id + ".ee_image"
                                dictje = aqueduct3.split_key(i_values_input_asset_id_extenstion,SCHEMA,SEPARATOR)
                                output_file_name = "{}_reduced_{:02.0f}_{}_{}".format(dictje["file_name"],pfaf_level,spatial_resolution,reducer_name)
                                output_file_path_pkl = "{}/{}.pkl".format(ec2_output_path,output_file_name)
                                output_file_path_csv = "{}/{}.csv".format(ec2_output_path,output_file_name)
                                
                                if os.path.isfile(output_file_path_pkl):
                                    message = "Index {:02.2f}, Skipping: {} Elapsed: {} Asset: {}".format(float(index),i_processed,str(timedelta(seconds=elapsed_time)),i_values_input_asset_id)
                                    logger.debug(message)
                                else:
                                    message = "Index {:02.2f}, Processed: {} Elapsed: {} Asset: {}".format(float(index),i_processed,str(timedelta(seconds=elapsed_time)),i_values_input_asset_id)
                                    print(message)
                                    logger.debug(message)
                                                     
                                    i_values = ee.Image(i_values_input_asset_id)
                                    total_image = ee.Image(i_values_input_asset_id).addBands(ee.Image(i_zones_asset_id))
                                    result_list = total_image.reduceRegion(geometry = geometry,
                                                    reducer= reducer,
                                                    crsTransform = crs_transform,
                                                    maxPixels=1e10
                                                    ).get("groups")

                                    function_properties = {"zones_pfaf_level":pfaf_level,
                                                           "zones_spatial_resolution":spatial_resolution,
                                                           "reducer":reducer_name,
                                                           "zones_image_asset_id":i_zones_asset_id}


                                    function_properties = {**function_properties, **dictje}
                                    df = post_process_results(result_list,function_properties)


                                    df.to_pickle(output_file_path_pkl)
                                    #df.to_csv(output_file_path_csv,encoding='utf-8')
 
                                    

if __name__ == "__main__":
    main()


# In[11]:

get_ipython().system("aws s3 cp {ec2_output_path} {s3_output_path} --recursive --exclude='*' --include='*.pkl'")


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:24:15.930678    
