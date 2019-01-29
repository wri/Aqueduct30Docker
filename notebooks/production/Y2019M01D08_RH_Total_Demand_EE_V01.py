
# coding: utf-8

# In[1]:

""" Total demand to be used as weights for spatial aggregation.
-------------------------------------------------------------------------------

Note that exporting global images including polar regions at a 5 arcminute 
resolution leads to internal errors. Using a maximum latitude of 89.5 which
corresponds to 4320 x 2148 pixels

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
OUTPUT_VERSION = 3

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160
Y_DIMENSION_5MIN_NOPOLAR = 2148 # was (2160)

X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600
Y_DIMENSION_30S_NOPOLAR = 21480 # was (21600)

CRS = "EPSG:4326"

CRS_TRANSFORM_5MIN_NOPOLAR = """[
    0.08333333333333333,
    0,
    -180,
    0,
    -0.08333333333333333,
    89.5
]"""

CRS_TRANSFORM_30S_NOPOLAR = """[
    0.008333333333333333,
    0,
    -180,
    0,
    -0.008333333333333333,
    89.5
]"""

dimensions_5min_nopolar = "{}x{}".format(X_DIMENSION_5MIN,Y_DIMENSION_5MIN_NOPOLAR)
dimensions_30s_nopolar = "{}x{}".format(X_DIMENSION_30S,Y_DIMENSION_30S_NOPOLAR)

ic_paths ={}
ic_paths["Dom"] = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_PDomWW_year_m_5min_1960_2014"
ic_paths["Ind"] = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_PIndWW_year_m_5min_1960_2014"
ic_paths["Irr"] = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_PIrrWW_year_m_5min_1960_2014"
ic_paths["Liv"] = "projects/WRI-Aquaduct/PCRGlobWB20V09/global_historical_PLivWW_year_m_5min_1960_2014"

print("ic_pdomww_path :",ic_paths["Dom"],
      "\nic_pindww_path :",ic_paths["Ind"],
      "\nic_pirrww_path :",ic_paths["Irr"],
      "\nic_plivww_path :",ic_paths["Liv"])


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

sectors = ["Dom","Ind","Irr","Liv"]


# In[6]:

reduced_image = {}
for sector in sectors:
    reduced_image[sector] = ee.ImageCollection(ic_paths[sector]).reduce(ee.Reducer.mean())


# In[7]:

reduced_image["Tot"] = reduced_image["Dom"].add(reduced_image["Ind"]).add(reduced_image["Ind"]).add(reduced_image["Irr"]).add(reduced_image["Liv"])


# In[8]:

sectors.append("Tot")


# In[9]:

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


# In[ ]:




# In[10]:

image_out = {}
for sector in sectors:
    print(sector)
    image_out[sector] = post_process(reduced_image[sector])


# In[11]:

command = "earthengine create folder projects/WRI-Aquaduct/{}".format(SCRIPT_NAME)
response = subprocess.check_output(command,shell=True)


# In[12]:

command = "earthengine create folder projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[14]:

for sector in sectors:
    print(sector)
    description_5min = "{}_{}_5min".format(SCRIPT_NAME,sector)
    asset_id_5min = 'projects/WRI-Aquaduct/{}/output_V{:02.0f}/global_historical_P{}WW_year_m_5min_1960_2014'.format(SCRIPT_NAME,OUTPUT_VERSION,sector)
    description_30s = "{}_{}_30s".format(SCRIPT_NAME,sector)
    asset_id_30s = 'projects/WRI-Aquaduct/{}/output_V{:02.0f}/global_historical_P{}WW_year_m_30s_1960_2014'.format(SCRIPT_NAME,OUTPUT_VERSION,sector)
    
    task_5min = ee.batch.Export.image.toAsset(
        image =  ee.Image(image_out[sector]),
        description = description_5min,
        assetId = asset_id_5min,
        dimensions = dimensions_5min_nopolar,
        crs = CRS,
        crsTransform = CRS_TRANSFORM_5MIN_NOPOLAR,
        maxPixels = 1e10   
    )
    
    task_5min.start()
    
    task_30s = ee.batch.Export.image.toAsset(
        image =  ee.Image(image_out[sector]),
        description = description_30s,
        assetId = asset_id_30s,
        dimensions = dimensions_30s_nopolar,
        crs = CRS,
        crsTransform = CRS_TRANSFORM_30S_NOPOLAR,
        maxPixels = 1e10   
    )
    task_30s.start()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:00:07.542663
# 

# In[ ]:



