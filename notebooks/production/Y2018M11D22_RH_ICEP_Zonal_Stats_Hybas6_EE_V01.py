
# coding: utf-8

# In[1]:

""" Zonal statistics icep_raw at hydrobasin level 6. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181122
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_
    NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.     

"""

TESTING = 0
SCRIPT_NAME = "Y2018M11D22_RH_ICEP_Zonal_Stats_Hybas6_EE_V01"
OUTPUT_VERSION = 1

EE_INPUT_ZONES_PATH = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_30s_V04"
EE_INPUT_VALUES_PATH = "projects/WRI-Aquaduct/Y2018M11D22_RH_ICEP_Basins_To_EE_V01/output_V01/icep_icepraw_30s"

EXTRA_PROPERTIES = {"output_version":OUTPUT_VERSION,
                    "script_used":SCRIPT_NAME,
                     "spatial_aggregation":"hydrobasin",
                     "parameter":"icep_raw",
                     "unit":"dimensionless"}


gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ee zones: " +  EE_INPUT_ZONES_PATH +
      "\nInput ee values: " + EE_INPUT_VALUES_PATH +
      "\nOutput s3: " + s3_output_path,
      "\nOutput gcs: " + gcs_output_path)


# In[2]:

import time, datetime, sys
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
import ee
import aqueduct3
ee.Initialize()


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

spatial_resolution = "30s"
reducer_name = "mean"
pfaf_level = 6


# In[7]:

geometry = aqueduct3.earthengine.get_global_geometry(TESTING)


# In[8]:

reducer = aqueduct3.earthengine.get_grouped_reducer(reducer_name)


# In[9]:

total_image = ee.Image(EE_INPUT_VALUES_PATH).addBands(ee.Image(EE_INPUT_ZONES_PATH))


# In[10]:

crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)


# In[11]:

result_list = total_image.reduceRegion(geometry = geometry,
                        reducer= reducer,
                        crsTransform = crs_transform,
                        maxPixels=1e10
                        ).get("groups")
            


# In[12]:

function_properties = {"spatial_resolution":spatial_resolution,
                       "reducer":reducer_name}


# In[13]:

df = post_process_results(result_list,function_properties)


# In[14]:

df.head()


# In[15]:

df.shape


# In[16]:

output_file_path_pkl = "{}/df_hybas_lev{:02.0f}_{}.pkl".format(ec2_output_path,pfaf_level,spatial_resolution)
output_file_path_csv = "{}/df_hybas_lev{:02.0f}_{}.csv".format(ec2_output_path,pfaf_level,spatial_resolution)
df.to_pickle(output_file_path_pkl)
df.to_csv(output_file_path_csv,encoding='utf-8')


# In[17]:

get_ipython().system('aws s3 cp  {ec2_output_path} {s3_output_path} --recursive')


# In[18]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:21.655136
# 
