
# coding: utf-8

# In[1]:

""" Sum riverdischarge at sinks at 5min resolution. 
-------------------------------------------------------------------------------

If a sub-basin contains one or more sinks (coastal and endorheic), the sum 
of riverdischarge at those sinks will be used. If a subbasin does not contain
any sinks or is too small to be represented at 5min, the main channel 
riverdischarge (30s validfa_mask) will be used. 

takes sum of riverdischarge at 5min sinks per zone. 

Author: Rutger Hofste
Date: 20180515
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.

"""

TESTING = 0
SCRIPT_NAME = "Y2018M05D15_RH_Sum_Riverdischarge_Sinks_5min_EE_V01"
OUTPUT_VERSION = 2

EXTRA_PROPERTIES = {"nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}
SEPARATOR = "_|-"
SCHEMA =["geographic_range",
         "temporal_range",
         "indicator",
         "temporal_resolution",
         "unit",
         "spatial_resolution",
         "temporal_range_min",
         "temporal_range_max"]


ZONES5MIN_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_5min_V04"
LDD_EE_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/global_lddsound_numpad_05min"
EE_INPUT_RIVERDISCHARGE_PATH_ID = "projects/WRI-Aquaduct/PCRGlobWB20V09/"

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

#!rm -r {ec2_output_path}
#!mkdir -p {ec2_output_path}


# In[4]:

# Imports
import pandas as pd
from datetime import timedelta
import os
import ee
import aqueduct3

ee.Initialize()


# In[5]:

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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/{}.log".format(SCRIPT_NAME))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[7]:

i_ldd_5min = ee.Image(LDD_EE_ASSET_ID)
i_sinks_5min =  i_ldd_5min.eq(5)
i_sinks_5min = i_sinks_5min.copyProperties(i_ldd_5min)
i_sinks_5min = i_sinks_5min.set("unit","boolean")


# In[8]:

temporal_resolutions = ["month","year"]
spatial_resolution = "5min"
pfaf_level = 6
indicator = "riverdischarge"
reducer_name = "sum"


# In[9]:

i_processed = 0
start_time = time.time()

# Zones Image
i_hybas_lev06_v1c_merged_fiona_5min = ee.Image(ZONES5MIN_EE_ASSET_ID)

# Geospatial constants
spatial_resolution = "5min"
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
            
            ####  ! Use sinks as mask ! ###
            i_values = i_values.mask(i_sinks_5min)
            
            result_list = aqueduct3.earthengine.raster_zonal_stats(
                                        i_zones = i_hybas_lev06_v1c_merged_fiona_5min,
                                        i_values = i_values,
                                        statistic_type = reducer_name,
                                        geometry = geometry_server_side,
                                        crs_transform = crs_transform,
                                        crs="EPSG:4326")
            
            function_properties = {"zones_pfaf_level":pfaf_level,
                                   "zones_spatial_resolution":spatial_resolution,
                                   "reducer":reducer_name,
                                   "zones_image_asset_id":ZONES5MIN_EE_ASSET_ID}

            function_properties = {**function_properties, **dictje}
            try:
                df_out = post_process_results(result_list,function_properties)
                df_out.to_pickle(output_file_path_pkl)
                df_out.to_csv(output_file_path_csv,encoding='utf-8')
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
# 0:48:09.013517
# 0:01:12.946037
# 

# In[ ]:



