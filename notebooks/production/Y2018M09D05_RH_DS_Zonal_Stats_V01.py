
# coding: utf-8

# In[1]:

""" Zonal stats for drought severity soil moisture and streamflow.
-------------------------------------------------------------------------------

Hydrobasin level 6


Author: Rutger Hofste
Date: 20180905
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

TESTING = 1
SCRIPT_NAME = "Y2018M09D05_RH_DS_Zonal_Stats_V01"
OUTPUT_VERSION = 1

ZONES_EE_PATH = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_V04"

EE_PATH_SOIL = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_droughtseveritystandardisedsoilmoisture_reduced_dimensionless_5min_1960_2014"
EE_PATH_STREAM = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_droughtseveritystandardisedstreamflow_reduced_dimensionless_5min_1960_2014"

GCS_BUCKET= "aqueduct30_v01"
GCS_OUTPUT_PATH = "{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("GCS_OUTPUT_PATH: ",GCS_OUTPUT_PATH)



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

geometry = aqueduct3.earthengine.get_global_geometry(TESTING)


# In[5]:

def reduce_region_soilmoisture(feature):
    i_value = ee.Image(EE_PATH_SOIL)
    geometry= feature.geometry()
    d = i_value.reduceRegion(reducer=ee.Reducer.mean(),
                             geometry=geometry,
                             scale = 1000,
                             bestEffort=False,
                             maxPixels=1e10,
                             tileScale=1)
    feature_out = ee.Feature(feature)
    mean = d.get("b1") 
    feature_out = feature_out.set("droughtseveritysoilmoisture",mean)
    return feature_out

def reduce_region_streamflow(feature):
    i_value = ee.Image(EE_PATH_STREAM)
    geometry= feature.geometry()
    d = i_value.reduceRegion(reducer=ee.Reducer.mean(),
                             geometry=geometry,
                             scale = 1000,
                             bestEffort=False,
                             maxPixels=1e10,
                             tileScale=1)
    feature_out = ee.Feature(feature)
    mean = d.get("b1") 
    feature_out = feature_out.set("droughtseveritystreamflow",mean)
    return feature_out


# In[6]:

fc_zones = ee.FeatureCollection(ZONES_EE_PATH)


# In[7]:

fc_reduced_soil = fc_zones.map(reduce_region_soilmoisture)


# In[8]:

fc_reduced_stream = fc_zones.map(reduce_region_streamflow)


# In[9]:

output_file_path_soil= "{}/droughtseveritysoilmoisture.csv".format(GCS_OUTPUT_PATH)
print(output_file_path_soil)
task_soil = ee.batch.Export.table.toCloudStorage(collection=fc_reduced_soil,
                                                 description="droughtseveritysoilmoisture",
                                                 bucket=GCS_BUCKET,
                                                 fileNamePrefix=output_file_path_soil,
                                                 fileFormat="CSV")
                                              


# In[10]:

task_soil.start()


# In[11]:

output_file_path_stream= "{}/droughtseveritystreamflow.csv".format(GCS_OUTPUT_PATH)
print(output_file_path_stream)
task_stream = ee.batch.Export.table.toCloudStorage(collection=fc_reduced_stream,
                                                 description="droughtseveritystreamflow",
                                                 bucket=GCS_BUCKET,
                                                 fileNamePrefix=output_file_path_stream,
                                                 fileFormat="CSV")


# In[12]:

task_stream.start()


# In[13]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 

# In[ ]:



