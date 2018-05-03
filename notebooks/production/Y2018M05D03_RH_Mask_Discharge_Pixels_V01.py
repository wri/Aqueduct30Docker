
# coding: utf-8

# In[1]:

""" Find the pixels to mask within each basin to use for river discharge. 
-------------------------------------------------------------------------------

Creates a mask for available river dicharge based on two criteria:

number of 30s pixels > threshold
sum_maxStreamorder < threshold


Args:

    COUNT_AREA_THRESHOLD_30S


"""

SCRIPT_NAME = "Y2018M05D03_RH_Mask_Discharge_Pixels_V01"

COUNT_AREA_THRESHOLD_30S = 1000 # corresponds to 10 5min cells
SUM_MAX_STREAMORDER_THRESHOLD_30S = 150 # corresponds to 1.5 5min cells


COUNT_AREA_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D02_RH_Prepare_Mask_Discharge_Pixels_V01/output_V02/global_count_area_dimensionless_30sPfaf06"
MAX_STREAMORDER_MASK_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D02_RH_Prepare_Mask_Discharge_Pixels_V01/output_V02/global_max_streamorder_mask_30sPfaf06"
SUM_MAXSTREAMORDER_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D02_RH_Prepare_Mask_Discharge_Pixels_V01/output_V02/global_sum_maxStreamorder_dimensionless_30sPfaf06"

TESTING = 0

OUTPUT_VERSION = 2

EXTRA_PROPERTIES = {"nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}


ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
print("Output ee: " +  ee_output_path)




# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import ee
import aqueduct3
ee.Initialize()


# In[4]:

i_count_area = ee.Image(COUNT_AREA_ASSET_ID)
i_sum_maxstreamorder = ee.Image(SUM_MAXSTREAMORDER_ASSET_ID)
i_max_streamorder_mask = ee.Image(MAX_STREAMORDER_MASK_ASSET_ID )


i_count_area_mask = i_count_area.gt(COUNT_AREA_THRESHOLD_30S)

i_sum_maxstreamorder_mask = i_sum_maxstreamorder.lt(SUM_MAX_STREAMORDER_THRESHOLD_30S)

i_mask = i_count_area_mask.multiply(i_sum_maxstreamorder_mask)




i_mask = i_mask.set(EXTRA_PROPERTIES)
i_mask = i_mask.set({"count_area_threshold_30s":COUNT_AREA_THRESHOLD_30S,
                     "sum_max_streamorder_threshold_30s":SUM_MAX_STREAMORDER_THRESHOLD_30S})

# i_mask is a per basin mask. Only masking out the pixels with max_streamorder



# In[5]:

# Geospatial constants
spatial_resolution = "30s"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)


# In[6]:

result = aqueduct3.earthengine.create_ee_folder_recursive(ee_output_path)


# In[7]:

key = "global_riverdischarge_mask_30sPfaf06"
output_asset_id = "{}/{}".format(ee_output_path,key)


task = ee.batch.Export.image.toAsset(
    image =  i_mask,
    assetId = output_asset_id,
    region = geometry_client_side,
    description = key,
    #dimensions = dimensions,
    crs = "EPSG:4326",
    crsTransform = crs_transform,
    maxPixels = 1e10     
)
task.start()


# In[8]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:07.640649
# 

# In[ ]:



