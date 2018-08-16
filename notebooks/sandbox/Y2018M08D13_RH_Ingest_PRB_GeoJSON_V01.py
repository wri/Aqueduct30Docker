
# coding: utf-8

# In[37]:

"""
Data needs to be stored in geoJSON file format. The type should be featureCollection.

Files are quite large so will have to come up with retrying 

"""
TESTING = 1

INPUT_FILE_PATH = "/volumes/data/Y2018M08D13_RH_Process_Basisregistratie_Gewaspercelen_V01/output_V05/"
EE_OUTPUT_PATH = "users/rutgerhofste/"

SCRIPT_NAME = "Y2018M08D13_RH_Ingest_PRB_GeoJSON_V01"
OUTPUT_VERSION = 2

EXTRA_PROPERTIES = {"script_used":SCRIPT_NAME,
                    "download_date":"2018-08-13",
                    "download_url":"http://www.nationaalgeoregister.nl/geonetwork/srv/dut/catalog.search#/metadata/%7B25943e6e-bb27-4b7a-b240-150ffeaa582e%7D?tab=relations"}


# In[17]:

import geojson
import json
import ee
import os
import subprocess

ee.Initialize()


# In[3]:

ee.__version__


# In[4]:

command = "earthengine create folder {}{}".format(EE_OUTPUT_PATH,SCRIPT_NAME)
response = subprocess.check_output(command,shell=True)
command = "earthengine create folder {}{}/output_V{:02.0f}".format(EE_OUTPUT_PATH,SCRIPT_NAME,OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[5]:




# In[6]:

data.crs


# In[40]:

files = os.listdir(INPUT_FILE_PATH)


# In[41]:

files


# In[42]:

if TESTING:
    files = files[0:2]


# In[44]:

for one_file in files:
    year = int(one_file[18:22]) #warning, file name dependent
    print(one_file)
    with open(INPUT_FILE_PATH + one_file, encoding="utf-8") as f:
        data = geojson.load(f)
        
        fc = data_to_featureCollection(data)


# In[45]:

def pre_process_properties(properties):
    # Optional additional step to convert datatypes, calculate derivatives etc.
    properties["GWS_GEWASCODE"] = int(properties["GWS_GEWASCODE"])
    properties["year"] = year
    return properties


# In[51]:

def data_to_featureCollection(data,batch_size=10):
    features = []
    for index , feature in enumerate(data["features"]):
        print(index)
        new_properties = pre_process_properties(feature.properties) 
        geometry = ee.Geometry(feature.geometry)    
        eefeature = ee.Feature(geometry,new_properties)
        features.append(eefeature)    
        if index == batch_size:
            fc = ee.FeatureCollection(features)   
            fc = fc.setMulti(EXTRA_PROPERTIES)
            break
    return fc


# In[52]:

fc = data_to_featureCollection(data)


# In[53]:

task = ee.batch.Export.table.toAsset(fc,
                                     description="Y2018M08D13_RH_Ingest_PRB_GeoJSON_V01",
                                     assetId = "{}{}/output_V{:02.0f}/Process_Basisregistratie_Gewaspercelen".format(EE_OUTPUT_PATH,SCRIPT_NAME,OUTPUT_VERSION))


# In[54]:

task.start()


# In[ ]:



