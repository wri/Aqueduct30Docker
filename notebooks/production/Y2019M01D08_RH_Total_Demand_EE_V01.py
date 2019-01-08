
# coding: utf-8

# In[1]:

""" Total demand to be used as weights for spatial aggregation.
-------------------------------------------------------------------------------

Creates total demand images. Will create a simple mean image for now but can be
extended to create imagecollections per sector etc. 

[ww,wn] -> ww
[year,month] -> year
[1960-2014] -> mean
[dom,ind,irr,liv]  -> total


Author: Rutger Hofste
Date: 20190108
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

"""


TESTING = 0
SCRIPT_NAME = "Y2019M01D08_RH_Total_Demand_EE_V01"
OUTPUT_VERSION = 1

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

CRS = "EPSG:4326"

CRS_TRANSFORM_5MIN = """[
    0.08333333333333333,
    0,
    -180,
    0,
    -0.08333333333333333,
    90
]"""

ic_pdomww_path = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_PDomWW_year_m_5min_1960_2014"
ic_pindww_path = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_PIndWW_year_m_5min_1960_2014"
ic_pirrww_path = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_PIrrWW_year_m_5min_1960_2014"
ic_plivww_path = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_PLivWW_year_m_5min_1960_2014"


print("ic_pdomww_path :",ic_pdomww_path,
      "\nic_pindww_path :",ic_pindww_path,
      "\nic_pirrww_path :",ic_pirrww_path,
      "\nic_plivww_path :",ic_plivww_path)


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

i_pdomww = ee.ImageCollection(ic_pdomww_path).reduce(ee.Reducer.mean())
i_pindww = ee.ImageCollection(ic_pindww_path).reduce(ee.Reducer.mean())
i_pirrww = ee.ImageCollection(ic_pirrww_path).reduce(ee.Reducer.mean())
i_plivww = ee.ImageCollection(ic_plivww_path).reduce(ee.Reducer.mean())


# In[6]:

i_ptotww = i_pdomww.add(i_pindww).add(i_pirrww).add(i_plivww)


# In[7]:

def post_process(image):
    image = image.select(["b1_mean"],["b1"])
    properties = {"reducer":"mean",
                  "year_min":1960,
                  "year_max":2014,
                  "unit":"m",
                  "spatial_resolution":"5min",
                  "script_used":SCRIPT_NAME,
                  "output_version":OUTPUT_VERSION     
    }
    image = image.set(properties)
    return image


# In[8]:

image_out = post_process(i_ptotww)


# In[9]:

command = "earthengine create folder projects/WRI-Aquaduct/{}".format(SCRIPT_NAME)
response = subprocess.check_output(command,shell=True)


# In[10]:

command = "earthengine create folder projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[11]:

description = SCRIPT_NAME
asset_id = 'projects/WRI-Aquaduct/{}/output_V{:02.0f}/global_historical_PTotWW_year_m_5min_1960_2014'.format(SCRIPT_NAME,OUTPUT_VERSION)


# In[12]:

asset_id


# In[13]:

dimensions_5min = "{}x{}".format(X_DIMENSION_5MIN,Y_DIMENSION_5MIN)


# In[14]:

task = ee.batch.Export.image.toAsset(
    image =  ee.Image(image_out),
    description = description,
    assetId = asset_id,
    dimensions = dimensions_5min,
    crs = CRS,
    crsTransform = CRS_TRANSFORM_5MIN,
    maxPixels = 1e10   
)


# In[15]:

task.start()


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:00:07.542663
# 

# In[ ]:



