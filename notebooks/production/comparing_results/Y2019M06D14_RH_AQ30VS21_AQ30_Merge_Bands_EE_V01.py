
# coding: utf-8

# In[25]:

""" Merge into one image with the indicators as bands.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190614
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2019M06D14_RH_AQ30VS21_AQ30_Merge_Bands_EE_V01"
OUTPUT_VERSION = 1

EE_INPUT_PATH = "projects/WRI-Aquaduct/Y2019M05D22_RH_AQ30VS21_Rasters_AQ30_Ingest_EE_V01/output_V03"

X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

CRS_TRANSFORM_30S = [
0.008333333333333333,
0,
-180,
0,
-0.008333333333333333,
90
]

CRS = "EPSG:4326"


# In[4]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[5]:

import ee


# In[6]:

ee.Initialize()


# In[21]:

indicators = ["bws_score",
              "bwd_score",
              "iav_score",
              "sev_score",
              "gtd_score",
              "rfr_score",
              "cfr_score",
              "drr_score",
              "ucw_score",
              "cep_score",
              "udw_score",
              "usa_score",
              "rri_score",
              "owr_score",
              "owr_wf",]


# In[22]:

one_image = ee.Image()
one_image = one_image.rename(["bws_score"])
for indicator in indicators:
    input_path = "{}/{}".format(EE_INPUT_PATH,indicator)
    image = ee.Image(input_path)
    image = image.select(["b1"],[indicator])
    one_image = one_image.addBands(srcImg=image,
                                   names=None,
                                   overwrite=True)


# In[27]:

ee_output_path = "projects/WRI-Aquaduct/Y2019M06D14_RH_AQ30VS21_AQ30_Merge_Bands_EE_V01/output_V01/aq30_v01"


# In[28]:

task_30s = ee.batch.Export.image.toAsset(
                                        image =  one_image,
                                        description = "test",
                                        assetId = ee_output_path,
                                        crs = CRS,
                                        crsTransform = CRS_TRANSFORM_30S,
                                        maxPixels = 1e10   
)


# In[29]:

task_30s.start()


# In[ ]:

# Hier gebleven. Test eerst met kleinere export transform. classic rookie mistake

