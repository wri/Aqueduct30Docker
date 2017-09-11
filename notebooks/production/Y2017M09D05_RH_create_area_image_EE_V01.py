
# coding: utf-8

# ### Create Area and ones Image EE
# 
# * Purpose of script: This notebooks create an area image (30s) in m2 to go from flux to volume and vice versa
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170905

# In[1]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[2]:

import ee
import numpy as np


# In[3]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07/"

DIMENSION5MIN = "4320x2160"
DIMENSION30S = "43200x21600"
CRS = "EPSG:4326"

VERSION = 11


# In[4]:

crsTransform5min = [
    0.08333333333333333,
    0,
    -180,
    0,
    -0.08333333333333333,
    90
]


# In[5]:

crsTransform30s = [
    0.008333333333333333,
    0,
    -180,
    0,
    -0.008333333333333333,
    90
  ]


# In[6]:

ee.Initialize()


# These "random" images are used to set the scales. These images were used because they were created using GDAL which is the most reliable way to create the rasters. 

# In[8]:

hybas_lev06_v1c_merged_fiona_30s_V01 = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev06_v1c_merged_fiona_30s_V01")
hybas_lev06_v1c_merged_fiona_5min_V01 = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev06_v1c_merged_fiona_5min_V01")


# In[9]:

scale30s = hybas_lev06_v1c_merged_fiona_30s_V01.projection().nominalScale().getInfo()
scale5min = hybas_lev06_v1c_merged_fiona_5min_V01.projection().nominalScale().getInfo()


# In[10]:

onesRaster = ee.Image.constant(1)
areaRaster = ee.Image.pixelArea()


# In[11]:

def exportToAsset(eePath, geometry,d):
    if d["spatial_resolution"] == "5min":
        crsTransform = crsTransform5min
    elif d["spatial_resolution"] == "30s":
        crsTransform = crsTransform30s
        
    image = d["image"]
    dimensions = d["dimensions"]
    
    metadata = d
    
    del metadata["image"]
    del metadata["dimensions"]
    
    
    image = image.set(metadata)    
    assetId = eePath + d["exportdescription"] + "V%0.2d" %(VERSION)
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = d["exportdescription"] + "V%0.2d" %(VERSION),
        assetId = assetId,
        dimensions = dimensions,
        #region = geometry.bounds().getInfo()['coordinates'][0],
        crs = CRS,
        crsTransform = crsTransform,
        maxPixels = 1e10
        )
    task.start()


# In[12]:

properties ={}


# In[13]:

properties["ones_5min"] = {"image":onesRaster,
                           "dimensions":DIMENSION5MIN,
                           "spatial_resolution":"5min",
                            "ingested_by":"RutgerHofste" ,
                            "exportdescription": "ones_5min" ,
                            "units": "dimensionless" ,
                            "script_used":"Y2017M09D05_RH_create_area_image_EE_V01",
                            "spatial_resolution":"5min"
                            }


# In[14]:

properties["ones_30s"] = {"image":onesRaster,
                          "dimensions":DIMENSION30S,
                          "spatial_resolution":"30s",
                          "ingested_by":"RutgerHofste",
                          "exportdescription": "ones_30s" ,
                          "units": "dimensionless",
                          "script_used":"Y2017M09D05_RH_create_area_image_EE_V01",
                          "spatial_resolution":"30s"
                            }


# In[15]:

properties["area_5min_m2"] = {"image":areaRaster,
                              "dimensions":DIMENSION5MIN,
                              "spatial_resolution":"5min",
                              "ingested_by":"RutgerHofste" ,
                              "exportdescription": "area_5min_m2" ,
                              "units": "m2",
                              "script_used":"Y2017M09D05_RH_create_area_image_EE_V01",
                              "spatial_resolution":"5min"
                             }


# In[16]:

properties["area_30s_m2"] = {"image":areaRaster,
                             "dimensions":DIMENSION30S,
                             "spatial_resolution":"30s",
                             "ingested_by":"RutgerHofste" ,
                             "exportdescription": "area_30s_m2" ,
                             "units": "m2",
                             "script_used":"Y2017M09D05_RH_create_area_image_EE_V01",
                             "spatial_resolution":"30s"
                             }


# In[17]:

for key, value in properties.iteritems():
    exportToAsset(EE_PATH, geometry,value)
    print(key)   

