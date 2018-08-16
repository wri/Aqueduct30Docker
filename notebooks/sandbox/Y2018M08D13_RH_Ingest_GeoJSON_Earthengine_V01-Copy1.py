
# coding: utf-8

# In[1]:

"""
Data needs to be stored in geoJSON file format. The type should be featureCollection.

Files are quite large so will have to come up with retrying 


"""

INPUT_FILE_NAME = "/volumes/data/Y2018M08D13_RH_Process_Basisregistratie_Gewaspercelen_V01/output_V01/BRP_Gewaspercelen_2015.json"


# In[2]:

import geojson
import json
import ee

ee.Initialize()


# In[3]:

ee.__version__


# In[4]:

with open(INPUT_FILE_NAME, encoding="utf-8") as f:
    data = geojson.load(f)


# In[5]:

type(data)


# In[10]:

data.crs


# In[6]:

len(data["features"])


# In[7]:

batch_size = 5
features = []
for index , feature in enumerate(data["features"]):
    properties = feature.properties
    geometry = feature.geometry
    geoJSONfeature = geojson.Feature(geometry=geometry,
                                     properties=properties)
    features.append(ee.Feature(geoJSONfeature))    
    if index == batch_size:
        fc = ee.FeatureCollection(features)
        task = ee.batch.Export.table.toAsset(fc,
                                             description="test",
                                             assetId = "users/rutgerhofste/test/test02")
                                             
        break


# In[8]:

task.start()


# In[ ]:



