
# coding: utf-8

# In[1]:

""" Find the pixels to mask within each basin to use for river discharge. 
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
SCRIPT_NAME = "Y2018M05D02_RH_Mask_Discharge_Pixels_V01"
STREAM_ORDER_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D25_RH_Ingest_Pcraster_GCS_EE_V01/output_V01/global_streamorder_dimensionless_05min_V02"
HYBAS_LEV06_30S_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_30s_V04"
AREA_M2_30S_ASSET_ID = "projects/WRI-Aquaduct/Y2017M09D05_RH_Create_Area_Image_EE_V01/output_V07/global_area_m2_30s_V07"
OUTPUT_VERSION = 1

COUNT_AREA_THRESHOLD_30S = 1000 # corresponds to 10 5min cells
SUM_MAX_STREAMORDER_THRESHOLD_30S = 150 # corresponds to 1.5 5min cells

SCHEMA = ["geographic_range",
      "indicator",
      "unit",
      "spatial_resolution",
      ]

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



# In[ ]:




# In[ ]:




# In[6]:

def master(i_zones,i_values,geometry,crs_transform,statistic_type):
    result_list = aqueduct3.earthengine.raster_zonal_stats(
                                            i_zones = i_zones,
                                            i_values = i_values,
                                            statistic_type = statistic_type,
                                            geometry = geometry_server_side,
                                            crs_transform = crs_transform,
                                            crs="EPSG:4326")
    i_result, i_count = aqueduct3.earthengine.zonal_stats_results_to_image(result_list,i_zones,statistic_type)
    
    i_dummy_result_properties = aqueduct3.earthengine.zonal_stats_image_propertes(i_zones,i_values,extra_properties={},zones_prefix="zones_",values_prefix="values_")
    
    i_result = i_result.multiply(1) #Deletes old properties
    i_result = i_result.copyProperties(i_dummy_result_properties)
    
    return i_result, i_count
    
    


# In[9]:

spatial_resolution = "30s"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=True)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)


i_zones = ee.Image(HYBAS_LEV06_30S_ASSET_ID)

i_global_area_m2_30s = ee.Image(AREA_M2_30S_ASSET_ID)
i_global_streamorder = ee.Image(STREAM_ORDER_ASSET_ID)

output_dict = {}

output_dict["sum_area"], output_dict["count_area"] = master(i_zones,i_global_area_m2_30s,geometry_client_side,crs_transform,"sum")
output_dict["max_streamorder"], output_dict["count_streamorder"] = master(i_zones,i_global_streamorder,geometry_client_side,crs_transform,"max")

# Find pixels @30s where stream_order == max_streamorder
i_max_streamorder_mask = ee.Image(output_dict["max_streamorder"]).eq(i_global_streamorder)
output_dict["max_streamorder_mask"] = i_max_streamorder_mask

output_dict["sum_max_streamorder"], output_dict["count_max_streamorder"] = master(i_zones,i_max_streamorder_mask,geometry_client_side,crs_transform,"sum")

i_count_area_mask = output_dict["count_area"].gt(COUNT_AREA_THRESHOLD_30S)
i_count_area_mask = i_count_area_mask.copyProperties(output_dict["count_area"],properties=["reducer","unit","zones","spatial_resolution","indicator"])
i_count_area_mask = ee.Image(i_count_area_mask)

i_sum_max_streamorder_mask = i_sum_max_streamorder.lt(SUM_MAX_STREAMORDER_THRESHOLD_30S)
i_sum_max_streamorder_mask = i_sum_max_streamorder_mask.copyProperties(i_sum_max_streamorder,properties=["reducer","unit","zones","spatial_resolution","indicator"])


i_mask = i_count_area_mask.multiply(i_sum_max_streamorder_mask)


# In[16]:




# In[ ]:

output_dict = {}





# Sum and count of area per zone
sum_area_result_list = aqueduct3.earthengine.raster_zonal_stats(
                                                i_zones = i_zones,
                                                i_values = i_global_area_m2_30s,
                                                statistic_type = "sum",
                                                geometry = geometry_server_side,
                                                crs_transform = crs_transform,
                                                crs="EPSG:4326")
i_sum_area, i_count_area = aqueduct3.earthengine.zonal_stats_results_to_image(sum_area_result_list,i_zones,"sum")
output_dict["i_sum_area"] = i_sum_area
output_dict["i_count_area"] = i_count_area

# Max streamorder per zone
result_list_max_streamorder = aqueduct3.earthengine.raster_zonal_stats(
                                                        i_zones = i_zones,
                                                        i_values = i_global_streamorder,
                                                        statistic_type = "max",
                                                        geometry = geometry_server_side,
                                                        crs_transform = crs_transform,
                                                        crs="EPSG:4326")

i_max_streamorder, i_count_streamorder  = aqueduct3.earthengine.zonal_stats_results_to_image(result_list_max_streamorder,i_zones,"max")
output_dict["i_max_streamorder"] = i_max_streamorder

i_max_streamorder_mask = i_max_streamorder.eq(i_global_streamorder)
output_dict["i_max_streamorder_mask"] = i_max_streamorder_mask

# Sum of max_streamorder. Number of 30s cells within zones that have the highest streamorder.
result_list_sum_max_streamorder = aqueduct3.earthengine.raster_zonal_stats(
                                                            i_zones = i_zones,
                                                            i_values = i_max_streamorder_mask,
                                                            statistic_type = "sum",
                                                            geometry = geometry_server_side,
                                                            crs_transform = crs_transform,
                                                            crs="EPSG:4326")
i_sum_max_streamorder, i_count_sum_max_streamorder  = aqueduct3.earthengine.zonal_stats_results_to_image(result_list_sum_max_streamorder,i_zones,"sum")
output_dict["i_sum_max_streamorder"] = i_sum_max_streamorder

i_count_area_mask = i_count_area.gt(COUNT_AREA_THRESHOLD_30S)
i_count_area_mask = i_count_area_mask.copyProperties(i_count_area,properties=["reducer","unit","zones","spatial_resolution","indicator"])
i_count_area_mask = ee.Image(i_count_area_mask)

i_sum_max_streamorder_mask = i_sum_max_streamorder.lt(SUM_MAX_STREAMORDER_THRESHOLD_30S)
i_sum_max_streamorder_mask = i_sum_max_streamorder_mask.copyProperties(i_sum_max_streamorder,properties=["reducer","unit","zones","spatial_resolution","indicator"])


i_mask = i_count_area_mask.multiply(i_sum_max_streamorder_mask)

output_dict["count_area_mask"] = i_count_area_mask
output_dict["sum_max_streamorder_mask"] = i_sum_max_streamorder_mask
output_dict["mask"] = i_mask


# In[ ]:

i_sum_max_streamorder.getInfo()


# In[ ]:

result = aqueduct3.earthengine.create_ee_folder_recursive(ee_output_path)


# In[ ]:

# rasters to export:
i_sum_area
i_count_area
i_max_streamorder
i_sum_max_streamorder
mask 



# In[ ]:

SCHEMA = ["geographic_range",
      "statistic_type",
      "indicator",
      "unit",
      "spatial_resolution",
      ]

i_sum_area = "global_sum_area_m2_30sPfaf06"
i_count_area = "global_count_area_m2_30sPfaf06"
i_max_streamorder = "global_max_streamorder_dimensionless_30sPfaf06"
i_sum_max_streamorder = "global_summax_streamorder_dimensionless_30sPfaf06"


# In[11]:

output_asset_id = "users/rutgerhofste/test/i_test"


# In[ ]:

dimensions = aqueduct3.earthengine.get_dimensions("30s")


# In[12]:

task = ee.batch.Export.image.toAsset(
    image =  i_result,
    assetId = output_asset_id,
    description = "area",
    region = geometry_client_side,
    #dimensions = dimensions,
    crs = "EPSG:4326",
    crsTransform = crs_transform,
    maxPixels = 1e10     
)
task.start()


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



