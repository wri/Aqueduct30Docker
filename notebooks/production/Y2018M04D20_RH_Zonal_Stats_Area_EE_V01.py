
# coding: utf-8

# In[1]:

""" Zonal statistics for basin area. Export in table format.
-------------------------------------------------------------------------------
Calculate the total area per basin in m2 for 5min and 30s resolution and for
level 00 and level 06 hydrobasins. Table is stored as csv and pickle on S3.


Author: Rutger Hofste
Date: 20180420
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
SCRIPT_NAME = "Y2018M04D20_RH_Zonal_Stats_Area_EE_V01"
EE_INPUT_ZONES_PATH = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V01"
EE_INPUT_VALUES_PATH = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02"
INPUT_VERSION_ZONES = 4
INPUT_VERSION_VALUES = 5
OUTPUT_VERSION = 2

EXTRA_PROPERTIES = {"output_version":OUTPUT_VERSION,
                    "script_used":SCRIPT_NAME,
                     "spatial_aggregation":"hydrobasin",
                     "parameter":"area",
                     "unit":"m2"}


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

import pandas as pd
import ee
import aqueduct3
ee.Initialize()


# In[4]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

def dict_to_feature(dictje):
    return ee.Feature(None,dictje)

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


# In[6]:

spatial_resolutions = ["5min","30s"]
pfaf_levels = [6,0]
reducer_names = ["sum"]

geometry = aqueduct3.earthengine.get_global_geometry(TESTING)

for reducer_name in reducer_names:
    reducer = aqueduct3.earthengine.get_grouped_reducer(reducer_name)
    
    for spatial_resolution in spatial_resolutions:
        crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)

        for pfaf_level in pfaf_levels:
            print(spatial_resolution,pfaf_level)
            
            i_zones_asset_id = "{}/hybas_lev{:02.0f}_v1c_merged_fiona_{}_V{:02.0f}".format(EE_INPUT_ZONES_PATH,pfaf_level,spatial_resolution,INPUT_VERSION_ZONES)
            i_values_asset_id = "{}/global_area_m2_{}_V{:02.0f}".format(EE_INPUT_VALUES_PATH,spatial_resolution,INPUT_VERSION_VALUES)
            
            #df = raster_zonal_stats(i_zones_asset_id,i_values_asset_id,geometry,crs_transform,reducer,output_type)
            
            
            total_image = ee.Image(i_values_asset_id).addBands(ee.Image(i_zones_asset_id))
            result_list = total_image.reduceRegion(geometry = geometry,
                                    reducer= reducer,
                                    crsTransform = crs_transform,
                                    maxPixels=1e10
                                    ).get("groups")
            
            function_properties = {"pfaf_level":pfaf_level,
                                   "spatial_resolution":spatial_resolution,
                                   "reducer":reducer_name}
            df = post_process_results(result_list,function_properties) 
            
            
            output_file_path_pkl = "{}/df_hybas_lev{:02.0f}_{}.pkl".format(ec2_output_path,pfaf_level,spatial_resolution)
            output_file_path_csv = "{}/df_hybas_lev{:02.0f}_{}.csv".format(ec2_output_path,pfaf_level,spatial_resolution)
            df.to_pickle(output_file_path_pkl)
            df.to_csv(output_file_path_csv,encoding='utf-8')


# In[7]:

df.head()


# In[8]:

get_ipython().system('aws s3 cp  {ec2_output_path} {s3_output_path} --recursive')


# In[9]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:01:06.446569
