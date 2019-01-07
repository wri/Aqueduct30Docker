
# coding: utf-8

# In[10]:

""" Zonal statistics icep_raw at GADM level 1.
-------------------------------------------------------------------------------

Calculate average Index for Coastal Eutrophication Potential raw value per 
gadm level 1 shape. 


Author: Rutger Hofste
Date: 20190107
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:


"""

TESTING = 1
SCRIPT_NAME = "Y2019M01D07_RH_GA_CEP_Zonal_Stats_GADM_EE_V01"
OUTPUT_VERSION = 1

EE_INPUT_ZONES_PATH = "projects/WRI-Aquaduct/Y2018D12D17_RH_GADM36L01_EE_V01/output_V02/gadm36l01"
EE_INPUT_VALUES_PATH = "projects/WRI-Aquaduct/Y2018M11D22_RH_ICEP_Basins_To_EE_V01/output_V01/icep_icepraw_30s"

EXTRA_PROPERTIES = {"output_version":OUTPUT_VERSION,
                    "script_used":SCRIPT_NAME,
                     "spatial_aggregation":"gadm_36_L01",
                     "parameter":"icep_raw",
                     "unit":"dimensionless"}

gcs_output_path = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ee zones: " +  EE_INPUT_ZONES_PATH +
      "\nInput ee values: " + EE_INPUT_VALUES_PATH +
      "\nOutput s3: " + s3_output_path,
      "\nOutput gcs: " + gcs_output_path)


# In[4]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[5]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[6]:

import pandas as pd
import ee
import aqueduct3
ee.Initialize()


# In[7]:

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


# In[8]:

spatial_resolution = "30s"
reducer_name = "mean"


# In[11]:

geometry = aqueduct3.earthengine.get_global_geometry(TESTING)


# In[14]:

reducer = aqueduct3.earthengine.get_grouped_reducer(reducer_name)


# In[15]:

total_image = ee.Image(EE_INPUT_VALUES_PATH).addBands(ee.Image(EE_INPUT_ZONES_PATH))


# In[ ]:

crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)

