
# coding: utf-8

# In[7]:

"""
Data needs to be stored in geoJSON file format. The type should be featureCollection.

"""

INPUT_FILE_NAME = "/volumes/data/Y2018M08D13_RH_Process_Basisregistratie_Gewaspercelen_V01/output_V01/BRP_Gewaspercelen_2015.json"


# In[6]:

import geojson
import json
import ee

ee.Initialize()


# In[19]:

with open(INPUT_FILE_NAME) as json_data:
    d = geojson.load(json_data.decode('utf-8').encode('utf-8')


# In[14]:




# In[ ]:



