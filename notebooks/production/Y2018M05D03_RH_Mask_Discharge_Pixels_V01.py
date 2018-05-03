
# coding: utf-8

# In[13]:

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
SUM_MAXSTREAMORDER_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D02_RH_Prepare_Mask_Discharge_Pixels_V01/output_V02/global_sum_maxStreamorder_dimensionless_30sPfaf06"

TESTING = 1

OUTPUT_VERSION = 1


ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
print("Output ee: " +  ee_output_path)




# In[9]:

import ee
import aqueduct3
ee.Initialize()


# In[7]:

i_count_area = ee.Image(COUNT_AREA_ASSET_ID)
i_sum_maxstreamorder = ee.Image(SUM_MAXSTREAMORDER_ASSET_ID)


i_count_area_mask = i_count_area.gt(COUNT_AREA_THRESHOLD_30S)

i_sum_maxstreamorder_mask = i_sum_maxstreamorder.lt(SUM_MAX_STREAMORDER_THRESHOLD_30S)

i_mask = i_count_area_mask.multiply(i_sum_maxstreamorder_mask)


# In[12]:

# Geospatial constants
spatial_resolution = "30s"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)


# In[ ]:




# In[ ]:

task = ee.batch.Export.image.toAsset(
    image =  image,
    assetId = output_asset_id,
    region = geometry_client_side,
    description = description,
    #dimensions = dimensions,
    crs = "EPSG:4326",
    crsTransform = crs_transform,
    maxPixels = 1e10     
)
task.start()

