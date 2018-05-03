
# coding: utf-8

# In[1]:

""" Create area and streamorder images that can be used to mask riverdischarge.
-------------------------------------------------------------------------------

The riverdischarge data from PCRGLOBWB is cummulative runoff. The available
discharge is the maximum discharge with a few exceptions. These exceptions
occur as a result of a mismatch of the PCRGLOBWB local drainage direction and
the hydrobasin level 6 subbasins. 

- A basin has a few cells that belong to another stream, 
    often close to the most downstream pixel.
- A basin has a few cells that are an endorheic upstream basin.

There are several subbasin types:

1) Large stream in only one cell. perpendicular contributing areas. 
    e.g.: 172265.
2) Large stream in a few cells, perpendicular contributing areas. 
    e.g.: 172263.
3) Tiny basin smaller than one 5min cell.
    e.g.: 172261.
4) Large basin with main stream.
    e.g.: 172250.
5) Large basin with main stream and other stream in most downstream cell. 
    e.g.: 172306.
6) Small basin with a confluence within basin. Stream_order increases in most 
    downstream cell but is part of basin. 
    e.g.: 172521.
7) Basin with an endorheac basin in one of its upstream cells.
    e.g.: 172144.
8) Large basin with main stream and other stream with large flow but lower 
    stream order.
    e.g.: To be found. 
    
The cell of highest streamorder will be masked for 5) and 7)
No mask will be applied to the other categories. 

This script creates several useful rasters to allow setting appropriate
thresholds for area and streamorder:

global_max_streamorder_dimensionless_30sPfaf06
global_sum_area_m2_30sPfaf06
global_count_maxStreamorder_dimensionless_30sPfaf06
global_max_streamorder_mask_30sPfaf06
global_count_area_dimensionless_30sPfaf06
global_count_streamorder_dimensionless_30sPfaf06
global_sum_maxStreamorder_dimensionless_30sPfaf06


Author: Rutger Hofste
Date: 20180502
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : Output version.

          


Returns:


"""

# Input Parameters
TESTING = 0
SCRIPT_NAME = "Y2018M05D02_RH_Prepare_Mask_Discharge_Pixels_V01"
STREAM_ORDER_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D25_RH_Ingest_Pcraster_GCS_EE_V01/output_V01/global_streamorder_dimensionless_05min_V02"
HYBAS_LEV06_30S_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_30s_V04"
AREA_M2_30S_ASSET_ID = "projects/WRI-Aquaduct/Y2017M09D05_RH_Create_Area_Image_EE_V01/output_V07/global_area_m2_30s_V07"
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

import pandas as pd
import ee
import aqueduct3
ee.Initialize()


# In[4]:

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
    
    


# In[5]:


# Geospatial constants
spatial_resolution = "30s"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)

# Zones and values images
i_zones = ee.Image(HYBAS_LEV06_30S_ASSET_ID)
i_global_area_m2_30s = ee.Image(AREA_M2_30S_ASSET_ID)
i_global_streamorder = ee.Image(STREAM_ORDER_ASSET_ID)


# Zonal Stats
output_dict = {}

output_dict["global_sum_area_m2_30sPfaf06"], output_dict["global_count_area_dimensionless_30sPfaf06"] = master(i_zones,i_global_area_m2_30s,geometry_client_side,crs_transform,"sum",{})
output_dict["global_max_streamorder_dimensionless_30sPfaf06"], output_dict["global_count_streamorder_dimensionless_30sPfaf06"] = master(i_zones,i_global_streamorder,geometry_client_side,crs_transform,"max",{})

# Find pixels @30s where stream_order == max_streamorder
output_dict["global_max_streamorder_mask_30sPfaf06"] = ee.Image(output_dict["global_max_streamorder_dimensionless_30sPfaf06"]).eq(i_global_streamorder)
output_dict["global_max_streamorder_mask_30sPfaf06"] = output_dict["global_max_streamorder_mask_30sPfaf06"].copyProperties(output_dict["global_max_streamorder_dimensionless_30sPfaf06"])
output_dict["global_max_streamorder_mask_30sPfaf06"] = output_dict["global_max_streamorder_mask_30sPfaf06"].set({"mask":"streamorder equals max streamorder"})

# Sum of max_streamorder @30s. Number of cells with maximum streamorder
output_dict["global_sum_maxStreamorder_dimensionless_30sPfaf06"], output_dict["global_count_maxStreamorder_dimensionless_30sPfaf06"] = master(i_zones,output_dict["global_max_streamorder_mask_30sPfaf06"],geometry_client_side,crs_transform,"sum",{})




# In[6]:

result = aqueduct3.earthengine.create_ee_folder_recursive(ee_output_path)

for key, value in output_dict.items():
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
    
    


# In[8]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:18.540335

# In[ ]:



