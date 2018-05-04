
# coding: utf-8

# In[1]:

""" Use flow accumulation and sinks to create second mask for riverdischarge.
-------------------------------------------------------------------------------

the previous script created a mask to deal with a PCRGLOBWB vs Hydrobasin
routing issue. Certain basins have, in addition to the main river, a couple
of sinks. These sinks are defined by the local drainage direction grid. 

this script:

1. Add endorheic sinks to mask from previous script.
    mask from previous script based on area and sum_max_streamorder thresholds.
2. apply mask to flow accumulation (FA) image.
3. find max masked_FA
    assumption is that riverdischarge available in the main stream occurs here.
4. apply mask to sinks image.
    mask from previous script.
5. add masked sinks to max(masked_FA)

Note that in earthengine the .mask() uses:
0 = invalid
1 = valid

so the mask 

Args:

"""


TESTING = 1
SCRIPT_NAME = "Y2018M05D03_RH_Max_FA_Add_Sinks_EE_V01"

MASK_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2018M05D03_RH_Mask_Discharge_Pixels_V01/output_V04/global_riverdischarge_mask_30sPfaf06"
FA_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2017M08D02_RH_Ingest_Aux_Rasters_GCS_EE_V02/output_V03/global_accumulateddrainagearea_km2_05min"
LDD_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2017M08D02_RH_Ingest_Aux_Rasters_GCS_EE_V02/output_V03/global_lddsound_numpad_05min"
ZONES_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_30s_V04"
ENDOSINKS_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2017M08D02_RH_Ingest_Aux_Rasters_GCS_EE_V02/output_V06/global_outletendorheicbasins_boolean_05min"





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

i_global_riverdischarge_mask_30sPfaf06 = ee.Image(MASK_EE_ASSET_ID)
i_global_accumulateddrainagearea_km2_05min = ee.Image(FA_EE_ASSET_ID)
i_global_lddsound_numpad_05min = ee.Image(LDD_EE_ASSET_ID)
i_hybas_lev06_v1c_merged_fiona_30s_V04 = ee.Image(ZONES_EE_ASSET_ID)
i_global_outletendorheicbasins_boolean_05min = ee.Image(ENDOSINKS_EE_ASSET_ID)


# In[5]:

def master(i_zones,i_values,geometry,crs_transform,statistic_type,extra_properties):
    result_list = aqueduct3.earthengine.raster_zonal_stats(
                                            i_zones = i_zones,
                                            i_values = i_values,
                                            statistic_type = statistic_type,
                                            geometry = geometry_server_side,
                                            crs_transform = crs_transform,
                                            crs="EPSG:4326")
    i_result, i_count = aqueduct3.earthengine.zonal_stats_results_to_image(result_list,i_zones,statistic_type)
    
    i_dummy_result_properties = aqueduct3.earthengine.zonal_stats_image_propertes(i_zones,i_values,extra_properties,zones_prefix="zones_",values_prefix="values_")
    
    i_result = i_result.multiply(1) #Deletes old properties
    i_result = i_result.copyProperties(i_dummy_result_properties)
    
    return i_result, i_count


# In[6]:

# 1. Add endorheic sinks to mask from previous script.
i_mask = i_global_riverdischarge_mask_30sPfaf06.add(i_global_outletendorheicbasins_boolean_05min)

# 2. apply mask to flow accumulation (FA) image.
i_global_accumulateddrainagearea_km2_05min_masked = i_global_accumulateddrainagearea_km2_05min.mask(i_mask)

# 3. find max masked_FA


# In[7]:

# Geospatial constants
spatial_resolution = "30s"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)



# In[8]:

i_result, i_count = master(i_zones = i_hybas_lev06_v1c_merged_fiona_30s_V04,
                           i_values = i_global_accumulateddrainagearea_km2_05min_masked,
                           geometry = geometry_client_side,
                           crs_transform = crs_transform,
                           statistic_type = "max",
                           extra_properties= {})



# In[10]:




# In[ ]:

print(key)
image = ee.Image(value)
image = image.setMulti(EXTRA_PROPERTIES)
description = key    
output_asset_id = "{}/{}".format(ee_output_path,key)

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

