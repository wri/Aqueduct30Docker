
# coding: utf-8

# In[6]:

""" Use the masked max riverdischarge flow accumulation and zones to create 
final masked zones image.
-------------------------------------------------------------------------------

The result of the previous step (Y2018M05D03_RH_Max_FA_Add_Sinks_EE_V01) is a 
raster with the FA in the non-masked pixels and 0 in everything else. 

This script uses this image as a mask that is applied to the 30sPfaf06 zones
image. 


TESTING (boolean) : Testing mode. Uses a smaller geography if enabled.
SCRIPT_NAME (string) : Script name.
OUTPUT_VERSION (Integer) : Output version.
EE_INPUT_ZONES_ASSET_ID (string) : earthengine input path for zones.
EE_INPUT_MAX_MASKEDFA_ASSET_ID (string) : earthengine input path for masks.
EE_INPUT_FA_ASSET_ID (string) : earthengine input path for flow accumulation.

EXTRA_PROPERTIES (dictionary) : Dictionary with extra properties for output image.


"""
TESTING = 0
SCRIPT_NAME = "Y2018M05D08_RH_Create_Zones_Mask_30sPfaf06_EE_V01"
OUTPUT_VERSION = 2

EE_INPUT_ZONES_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_30s_V04"
EE_INPUT_MAX_MASKEDFA_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D03_RH_Max_FA_Add_Sinks_EE_V01/output_V02/global_max_maskedaccumulateddrainagearea_km2_30sPfaf06"
EE_INPUT_FA_ASSET_ID = "projects/WRI-Aquaduct/Y2017M08D02_RH_Ingest_Aux_Rasters_GCS_EE_V02/output_V06/global_accumulateddrainagearea_km2_05min"



EXTRA_PROPERTIES = {"output_version":OUTPUT_VERSION,
                    "script_used":SCRIPT_NAME,
                    "mask":EE_INPUT_MAX_MASKEDFA_ASSET_ID
                   }


# Output Parameters
ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


print("Input ee zones: " +  EE_INPUT_ZONES_ASSET_ID +
      "\nInput ee mask: " + EE_INPUT_MAX_MASKEDFA_ASSET_ID + 
      "\nInput fa: " + EE_INPUT_FA_ASSET_ID +
      "\nOutput ee path: " + ee_output_path)


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

# Images
i_zones_30sPfaf06 = ee.Image(EE_INPUT_ZONES_ASSET_ID)
i_fa_km2_05min = ee.Image(EE_INPUT_FA_ASSET_ID)
i_max_maskedfa_km2_30sPfaf06 = ee.Image(EE_INPUT_MAX_MASKEDFA_ASSET_ID)

# 0. Mask from previous script: (Y2018M05D03_RH_Max_FA_Add_Sinks_EE_V01 == flow accumulation ) 
i_mask_max_maskedfa_boolean_30sPfaf06 =  i_max_maskedfa_km2_30sPfaf06.eq(i_fa_km2_05min)
i_masked_zones_30sPfaf06 = i_zones_30sPfaf06.mask(i_mask_max_maskedfa_boolean_30sPfaf06.neq(0))
i_masked_zones_30sPfaf06 = i_masked_zones_30sPfaf06.setMulti(EXTRA_PROPERTIES)

result = aqueduct3.earthengine.create_ee_folder_recursive(ee_output_path)

key = "validmaxfa_hybas_lev06_v1c_merged_fiona_30s_V04"
description = key
output_asset_id = "{}/{}".format(ee_output_path,key)

spatial_resolution = "30s"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)

task = ee.batch.Export.image.toAsset(
    image =  i_masked_zones_30sPfaf06,
    assetId = output_asset_id,
    region = geometry_client_side,
    description = description,
    #dimensions = dimensions,
    crs = "EPSG:4326",
    crsTransform = crs_transform,
    maxPixels = 1e10     
)
task.start()


# In[5]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:08.885712

# In[ ]:



