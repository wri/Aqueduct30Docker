
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

TESTING = 0
SCRIPT_NAME = "Y2018M05D04_RH_Zonal_Stats_Supply_EE_V01"
OUTPUT_VERSION = 3

EE_INPUT_ZONES_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D08_RH_Create_Zones_Mask_30sPfaf06_EE_V01/output_V02/validmaxfa_hybas_lev06_v1c_merged_fiona_30s_V04"
EE_INPUT_RIVERDISCHARGE_PATH_ID = "projects/WRI-Aquaduct/PCRGlobWB20V09/"

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

print("Input ee zones: " +  EE_INPUT_ZONES_ASSET_ID +
      "\nInput ee values path: " + EE_INPUT_RIVERDISCHARGE_PATH_ID  +
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


# In[6]:

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


# In[7]:

# 1. first riverdischarge in zones masked by previous script (max_fa)


# In[8]:

temporal_resolutions = ["month","year"]
spatial_resolution = "30s"
pfaf_level = 6
indicator = "riverdischarge"
reducer_name = "first"


# In[9]:

i_processed = 0
start_time = time.time()

# Zones Image
i_zones_30sPfaf06 = ee.Image(EE_INPUT_ZONES_ASSET_ID)

# Geospatial constants
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']

crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)

for temporal_resolution in temporal_resolutions:
    ic_values_input_asset_id = "{}global_historical_{}_{}_millionm3_5min_1960_2014".format(EE_INPUT_RIVERDISCHARGE_PATH_ID,indicator,temporal_resolution)
    print(ic_values_input_asset_id)
    df = aqueduct3.earthengine.get_df_from_ic(ic_values_input_asset_id)

    if TESTING:
        df = df[0:3]
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
            
            
            result_list = aqueduct3.earthengine.raster_zonal_stats(
                                        i_zones = i_zones_30sPfaf06,
                                        i_values = i_values,
                                        statistic_type = reducer_name,
                                        geometry = geometry_server_side,
                                        crs_transform = crs_transform,
                                        crs="EPSG:4326")
            
            function_properties = {"zones_pfaf_level":pfaf_level,
                                   "zones_spatial_resolution":spatial_resolution,
                                   "reducer":reducer_name,
                                   "zones_image_asset_id":EE_INPUT_ZONES_ASSET_ID}

            function_properties = {**function_properties, **dictje}
            try:
                df = post_process_results(result_list,function_properties)
                df.to_pickle(output_file_path_pkl)
                #df.to_csv(output_file_path_csv,encoding='utf-8')
            except:
                message = "Index {:02.2f}, Error: {} Elapsed: {} Asset: {}".format(float(index),i_processed,str(timedelta(seconds=elapsed_time)),i_values_input_asset_id)
                logger.debug(message)


# In[10]:

get_ipython().system("aws s3 cp {ec2_output_path} {s3_output_path} --recursive --exclude='*' --include='*.pkl'")


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 4:13:42.695557 (internal server errors, had to manually restart, half run)
# 
# 

# In[ ]:



