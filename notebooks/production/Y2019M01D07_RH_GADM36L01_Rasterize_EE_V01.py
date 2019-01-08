
# coding: utf-8

# In[13]:

""" Rasterize GADM level 1 using earthengine.
-------------------------------------------------------------------------------

Rasterization in GDAL took way too long, probably due to high level of detail
of GADM.  Let's use Earthengine muscles!

Author: Rutger Hofste
Date: 20190107
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

"""

TESTING = 0
SCRIPT_NAME = "Y2019M01D07_RH_GADM36L01_Rasterize_EE_V01"
OUTPUT_VERSION = 1

EE_FC_INPUT_PATH = "projects/WRI-Aquaduct/Y2018D12D17_RH_GADM36L01_EE_V01/output_V06/gadm36l01"

CRS = "EPSG:4326"
X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

CRS_TRANSFORM_30S = """[
    0.008333333333333333,
    0,
    -180,
    0,
    -0.008333333333333333,
    90
  ]"""


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import ee
import subprocess


# In[4]:

ee.Initialize()


# In[5]:

dimensions_30s = "{}x{}".format(X_DIMENSION_30S,Y_DIMENSION_30S)


# In[6]:

fc = ee.FeatureCollection(EE_FC_INPUT_PATH)


# In[7]:

image = ee.Image(fc.reduceToImage(properties=["gid_1_id"],
                                  reducer=ee.Reducer.mode()))


# In[8]:

command = "earthengine create folder projects/WRI-Aquaduct/{}".format(SCRIPT_NAME)
response = subprocess.check_output(command,shell=True)


# In[9]:

command = "earthengine create folder projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[10]:

asset_id = "projects/WRI-Aquaduct/{}/output_V{:02.0f}/{}".format(SCRIPT_NAME,OUTPUT_VERSION,SCRIPT_NAME)


# In[14]:

task= ee.batch.Export.image.toAsset(image=image,
                                    description=SCRIPT_NAME,
                                    assetId =asset_id,
                                    dimensions = dimensions_30s,
                                    crs =CRS,
                                    crsTransform = CRS_TRANSFORM_30S,
                                    maxPixels = 1e10
                                   )


# In[15]:

task.start()


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 
