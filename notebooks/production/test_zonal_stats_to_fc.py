
# coding: utf-8

# In[1]:

# Test zonal stats to fc


# In[2]:

import aqueduct3


# In[3]:

EE_INPUT_ZONES_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D08_RH_Create_Zones_Mask_30sPfaf06_EE_V01/output_V02/validmaxfa_hybas_lev06_v1c_merged_fiona_30s_V04"
EE_INPUT_RIVERDISCHARGE_PATH_ID = "projects/WRI-Aquaduct/PCRGlobWB20V09/"


# In[5]:

# Imports
import pandas as pd
from datetime import timedelta
import os
import ee
import aqueduct3

ee.Initialize()


# In[21]:

i_zones = ee.Image(EE_INPUT_ZONES_ASSET_ID)


# In[15]:

ic_test_asset_id = "{}global_historical_riverdischarge_month_millionm3_5min_1960_2014".format(EE_INPUT_RIVERDISCHARGE_PATH_ID)
ic_test = ee.ImageCollection(ic_test_asset_id)


# In[16]:

i_test = ee.Image(ic_test.first())


# In[19]:

TESTING = 1

geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']

crs_transform = aqueduct3.earthengine.get_crs_transform("30s")


# In[22]:

result_list = aqueduct3.earthengine.raster_zonal_stats(
                            i_zones = i_zones,
                            i_values = i_test,
                            statistic_type = "first",
                            geometry = geometry_server_side,
                            crs_transform = crs_transform,
                            crs="EPSG:4326")


# In[32]:


def zonal_stats_results_to_fc(result_list):
    fc = result_list.map(lambda x: ee.Feature(None,x))
    return ee.FeatureCollection(fc)
    
    


# In[37]:

fc = zonal_stats_results_to_fc(result_list)


# In[45]:




# In[51]:




# In[63]:

import ee
ee.Initialize()

feature01 = ee.Feature(ee.Geometry.Point(1,2),{"foo":1,"bar":2})
feature02 = ee.Feature(ee.Geometry.Point(3,4),{"foos":41,"bars":42})
feature03 = ee.Feature(None,{"fooz":None,"bars":-9999})
feature04 = ee.Feature(None,{"fooz":None,"bars":-9999})

fc1 = ee.FeatureCollection([feature01,feature02])
fc2 = ee.FeatureCollection([feature02,feature03])
fc3 = ee.FeatureCollection([feature03,feature04])

fc_merged = fc1.merge(fc2)

taskParams = {'json' : fc3.serialize(), 'type': 'EXPORT_FEATURES', 'assetId': 'users/rutgerhofste/pythontest3','description': 'pyhtonnullgeometry'}  
taskId = ee.data.newTaskId()
ee.data.startProcessing(taskId[0], taskParams)


# In[58]:




# In[ ]:



