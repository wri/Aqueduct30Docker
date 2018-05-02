
# coding: utf-8

# In[1]:

""" Find the pixels to mask within each basin to use for river discharge. 
-------------------------------------------------------------------------------

The riverdischarge data from PCRGLOBWB is cummulative runoff. The available
discharge is the maximum discharge with a few exceptions. These exceptions
occur as a result of a mismatch of the PCRGLOBWB local drainage direction and
the hydrobasin level 6 subbasins. 

- A basin has a few cells that belong to another stream. 
    Often close to the most downstream pixel.
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
HYBAS_LEV06_30S_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V01/hybas_lev06_v1c_merged_fiona_30s_V04"
GLOBAL_AREA_M2_30S_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/global_area_m2_30s_V05"
OUTPUT_VERSION = 1

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


# Count pixels per basin and total area
spatial_resolution = "30s"
total_image = ee.Image(GLOBAL_AREA_M2_30S_ASSET_ID).addBands(ee.Image(HYBAS_LEV06_30S_ASSET_ID))
reducer = ee.Reducer.sum().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=True)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)




# In[5]:

def ensure_default_properties(obj): 
    obj = ee.Dictionary(obj)
    default_properties = ee.Dictionary({"mean": -9999,"count": -9999,"max":-9999})
    return default_properties.combine(obj)

def map_list(results, key):
    new_result = results.map(lambda x: ee.Dictionary(x).get(key))
    return new_result


def raster_zonal_stats(i_zones,i_values,statistic_type,geometry, crs_transform,crs="EPSG:4326"):
    """ Zonal Statistics using raster zones and values.
    -------------------------------------------------------------------------------
    Output options include a ee.FeatureCollection, pd.DataFrame or ee.Image.
    The count is always included in the output results. 
    
    crs transform can be obtained by the get_crs_transform function.
    
    Note that if a zone does not contain data, the value is missing from the
    dictionary in the list.
    
    Args:
        i_zones (ee.Image) : Integer image with zones.
        i_values (ee.Image) : Image with values.
        statistic_type (string) : Statistics type like 'mean', 'sum'.
        geometry (ee.Geometry) : Geometry defining extent of calculation.
        nodata_value (integer) : nodata value. Defaults to -9999.
        crs_transform (list) : crs transform. 
        crs (string) : crs, deafults to 'EPSG:4326'.
        
    Returns:
        result_list (ee.List) : list of dictionaries with keys 'zones','count'
            and 'mean/max/sum etc.'. 
            
    """
    
    
    if statistic_type == "mean":
        reducer = ee.Reducer.mean().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")
    elif statistic_type == "max":
        reducer = ee.Reducer.max().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")
    elif statistic_type == "sum":
        reducer = ee.Reducer.sum().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")
    else:
        raise UserWarning("Statistic_type not yet supported, please modify function")
    
    total_image = ee.Image(i_values).addBands(ee.Image(i_zones))
    
    result_list = total_image.reduceRegion(
        geometry = geometry_server_side,
        reducer= reducer,
        crsTransform = crs_transform,
        crs = crs,
        maxPixels=1e10
        ).get("groups")

    result_list = ee.List(result_list)    
    
    
    return result_list



def zonal_stats_results_to_image(result_list,i_zones,statistic_type):
    """ Map result list on zones image to get result image.
    -------------------------------------------------------------------------------
    
    Args:
        result_list (ee.List) : list of dictionaries.
        i_zones (ee.Image) : zones image.
        statistic_type (string) : Statistics type like 'mean', 'sum'.
        
    Returns:
        i_result (ee.Image) : result image with three bands 'zones','count' and
            the statistical_type from result list.
    
    """
    result_list = ee.List(result_list)
    result_list = result_list.map(ensure_default_properties)
    
    zone_list = map_list(result_list, 'zones')
    count_list = map_list(result_list,"count")
    stat_list = map_list(result_list,statistic_type)
    
    
    count_image = ee.Image(i_zones).remap(zone_list, count_list).select(["remapped"],["count"])
    
    
    stat_image = ee.Image(i_zones).remap(zone_list, stat_list).select(["remapped"],[statistic_type])
    

    
    result_image = i_zones.addBands(count_image).addBands(stat_image)
    properties = {"statistic_type":statistic_type}
    result_image = result_image.set(properties) 
    


# In[6]:

result_image = raster_zonal_stats(
                i_zones = ee.Image(HYBAS_LEV06_30S_ASSET_ID),
                i_values = ee.Image(GLOBAL_AREA_M2_30S_ASSET_ID),
                statistic_type = "sum",
                geometry = geometry_server_side,
                crs_transform = crs_transform,
                crs="EPSG:4326")


# In[7]:

output_asset_id = "users/rutgerhofste/test/test"


# In[8]:

dimensions = aqueduct3.earthengine.get_dimensions("30s")


# In[13]:

task = ee.batch.Export.image.toAsset(
    image =  result_image,
    assetId = output_asset_id,
    description = "test",
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



