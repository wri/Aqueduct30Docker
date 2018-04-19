
# coding: utf-8

# In[1]:

""" Create Area and ones Image EE
-------------------------------------------------------------------------------
create an area image (30s and 5min) in m2 to go from flux to volume and vice 
versa. Also creates ones images. 

Author: Rutger Hofste
Date: 20170905
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name.
    INPUT_VERSION (string) : Input version.
    OUTPUT_VERSION (string) : Output version.
    DIMENSIONS_5MIN (string) : Dimensions for global 5 arc minute data
      in string format: '1234x1234'.
    DIMENSIONS_30S (string) : Dimensions for global 30 arc second data
      in string format: '1234x1234'.
    CRS (string) : Coordinate Reference System in string format using 'EPSG:'.
    EXTRA_PROPERTIES (Dictionary) : Extra properties to add to assets. nodata_value,
      script used are common properties.

Returns:

TODO:

- remove the need to specify transform if dimension is specified.
- remove some unnecessary iteration in dictionary specification.


"""


SCRIPT_NAME = "Y2017M09D05_RH_Create_Area_Image_EE_V01"
INPUT_VERSION = 2
OUTPUT_VERSION = 5 

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160
X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600
CRS = "EPSG:4326"


EXTRA_PROPERTIES = {
"ingested_by" : "RutgerHofste",
"script_used": SCRIPT_NAME,
"output_version":OUTPUT_VERSION   
}

CRS_TRANSFORM_5MIN = [
    0.08333333333333333,
    0,
    -180,
    0,
    -0.08333333333333333,
    90
]

CRS_TRANSFORM_30S = [
    0.008333333333333333,
    0,
    -180,
    0,
    -0.008333333333333333,
    90
  ]


ee_path = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V{:02.0f}".format(INPUT_VERSION)

print("Input ee: " +  ee_path +
      "\nOutput ee: " + ee_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# imports
import ee
import numpy as np
import aqueduct3

ee.Initialize()


# In[4]:

def exportToAsset(ee_path,d):
    """ Export image to asset

    Args:
        ee_path (string) : earth engine folder.
        d (dictionary) : dictionary with properties. Required:
          'image'  and 'dimensions'
    
    Returns:
        None
    
    """
    
    
    if d["spatial_resolution"] == "5min":
        crsTransform = CRS_TRANSFORM_5MIN
    elif d["spatial_resolution"] == "30s":
        crsTransform = CRS_TRANSFORM_30S
        
    image = d["image"]
    dimensions = d["dimensions"]
    
    metadata = d
    
    del metadata["image"]
    del metadata["dimensions"]
    
    
    image = image.set(metadata)    
    assetId = ee_path + "/" + d["exportdescription"] + "_V{:02.0f}".format(OUTPUT_VERSION)
    
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = d["exportdescription"] + "_V{:02.0f}".format(OUTPUT_VERSION),
        assetId = assetId,
        dimensions = dimensions,
        #region = geometry.bounds().getInfo()['coordinates'][0],
        crs = CRS,
        crsTransform = crsTransform,
        maxPixels = 1e10
        )
    task.start()
    
    
dimensions_5min = "{}x{}".format(X_DIMENSION_5MIN,Y_DIMENSION_5MIN)
dimensions_30s = "{}x{}".format(X_DIMENSION_30S,Y_DIMENSION_30S)    


ones_raster = ee.Image.constant(1)
area_raster = ee.Image.pixelArea()


# In[5]:

properties ={}


# In[6]:

properties["global_ones_5min"] = {"image":ones_raster,
                           "dimensions":dimensions_5min,
                           "spatial_resolution":"5min",
                           "ingested_by":"RutgerHofste" ,
                           "exportdescription": "global_ones_5min" ,
                           "unit": "dimensionless" ,
                           "script_used":SCRIPT_NAME,
                           "spatial_resolution":"5min",
                           "output_version":OUTPUT_VERSION
                           }


# In[7]:

properties["global_ones_30s"] = {"image":ones_raster,
                          "dimensions":dimensions_30s,
                          "spatial_resolution":"30s",
                          "ingested_by":"RutgerHofste",
                          "exportdescription": "global_ones_30s" ,
                          "unit": "dimensionless",
                          "script_used":SCRIPT_NAME,
                          "spatial_resolution":"30s",
                          "output_version":OUTPUT_VERSION
                          }


# In[8]:

properties["global_area_m2_5min"] = {"image":area_raster,
                              "dimensions":dimensions_5min,
                              "spatial_resolution":"5min",
                              "ingested_by":"RutgerHofste" ,
                              "exportdescription": "global_area_m2_5min" ,
                              "unit": "m2",
                              "script_used":SCRIPT_NAME,
                              "spatial_resolution":"5min",
                              "output_version":OUTPUT_VERSION
                             }


# In[9]:

properties["global_area_m2_30s"] = {"image":area_raster,
                             "dimensions":dimensions_30s,
                             "spatial_resolution":"30s",
                             "ingested_by":"RutgerHofste" ,
                             "exportdescription": "global_area_m2_30s" ,
                             "unit": "m2",
                             "script_used":SCRIPT_NAME,
                             "spatial_resolution":"30s",
                             "output_version":OUTPUT_VERSION
                             }


# In[10]:

for key, value in properties.items():
    exportToAsset(ee_path,value)
    print(key)   


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:08.226092

# In[ ]:



