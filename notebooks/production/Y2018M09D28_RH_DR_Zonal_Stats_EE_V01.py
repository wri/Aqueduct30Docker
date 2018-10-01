
# coding: utf-8

# In[1]:

""" Drought Risk zonal stats in earthengine.
-------------------------------------------------------------------------------



Author: Rutger Hofste
Date: 201809028
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
"""

SCRIPT_NAME = "Y2018M09D28_RH_DR_Zonal_Stats_EE_V01"
EE_INPUT_ZONES_PATH = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_V04"
EE_INPUT_VALUES_PATH = "projects/WRI-Aquaduct/Y2018M09D28_RH_DR_Ingest_EE_V01/output_V03/"

OUTPUT_VERSION = 1

GCS_BUCKET= "aqueduct30_v01"

print("Input ee zones: " +  EE_INPUT_ZONES_PATH +
      "\nInput ee values: " + EE_INPUT_VALUES_PATH +
      "\nGCS_BUCKET: " + GCS_BUCKET)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

def dict_to_feature(dictje):
    return ee.Feature(None,dictje)

def feature_remove_geometry(feature):
    feature_out = feature.setGeometry(None)
    return feature_out


# In[4]:

import ee
import aqueduct3
ee.Initialize()


# In[5]:

reducer = ee.Reducer.count().combine(
  reducer2= ee.Reducer.mean(),
  sharedInputs= True
)


# In[6]:

# parameters = ["desertcoldareamask","hazard","exposure","vulnerability","risk"]
# removed mask since it will not output a mean column. 

parameters = ["hazard","exposure","vulnerability","risk"]


# In[7]:

fc_hydrobasins = ee.FeatureCollection("projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_V04");
crs_transform = aqueduct3.earthengine.get_crs_transform("5min")

for parameter in parameters:
    print(parameter)
    input_image = ee.Image("projects/WRI-Aquaduct/Y2018M09D28_RH_DR_Ingest_EE_V01/output_V03/{}".format(parameter))
    fc_stats = input_image.reduceRegions(collection=fc_hydrobasins,
                                  reducer=reducer,
                                  crsTransform=crs_transform )
    fc_stats_nogeom = fc_stats.map(feature_remove_geometry)
    file_name_prefix = "{}/output_V{:02.0f}/{}".format(SCRIPT_NAME,OUTPUT_VERSION,parameter)
    print(file_name_prefix)
    description = "{}_V{:02.0f}".format(parameter,OUTPUT_VERSION)
    task = ee.batch.Export.table.toCloudStorage(collection = fc_stats_nogeom,
                                                description=description,
                                                bucket=GCS_BUCKET,
                                                fileNamePrefix=file_name_prefix,
                                                fileFormat="CSV"
                                                )
    task.start()


# In[8]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:10.984084  
# 0:00:07.563100

# In[ ]:



