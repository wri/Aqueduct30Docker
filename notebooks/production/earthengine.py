""" Aqueduct Function Codebase Submodule
-------------------------------------------------------------------------------
DO NOT MOVE/RENAME THIS FILE
this is not the official earthengine module.

frequently used functions for Aqueduct Jupyter Notebooks. 
This is a submodule for Aqueduct. 

TODO:
- move codebase to pypi and conda
- remove some hardcoded dependencies. 


Author: Rutger Hofste
Date: 20180329
Kernel: N/A
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

import subprocess
import re
import ee
import pandas as pd
from retrying import retry
from calendar import monthrange, isleap
ee.Initialize()


# Global Variables 

GLOBAL_AREA_M2_5MIN_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/global_area_m2_5min_V05"

X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160
X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600
CRS = "EPSG:4326"

CRS_TRANSFORM_5MIN = [
    0.08333333333333333,
    0,
    -180,
    0,
    -0.08333333333333333,
    90
]

CRS_TRANSFORM_30S = [
    0.008333333333333333,
    0,
    -180,
    0,
    -0.008333333333333333,
    90
]

dimensions_5min = "{}x{}".format(X_DIMENSION_5MIN,Y_DIMENSION_5MIN)
dimensions_30s = "{}x{}".format(X_DIMENSION_30S,Y_DIMENSION_30S)    

def create_ee_folder_recursive(ee_output_path,overwrite=False,min_depth=3):
    sep = "/"
    tree = [sep+x for x in ee_output_path.split(sep)]
    depth = min_depth
    
    while depth <= len(tree):
        path =  ''.join(tree[0:depth])
        path = path[1:]
        command = "earthengine create folder {}".format(path)
        result = subprocess.check_output(command,shell=True)
        print(command,result)
        depth = depth + 1
    return result

def create_imageCollection(ic_id):
    """ Creates an imageCollection using command line
    -------------------------------------------------------------------------------
    Args:
        ic_id (string) : asset_id of image Collection.
        
    Returns:
        command (string) : command parsed to subprocess module 
        result (string) : subprocess result.
    """
    command = "earthengine create collection {}".format(ic_id)
    result = subprocess.check_output(command,shell=True)
    return command, result

def volume_to_flux_5min_millionm3_m2(image_volume_millionm3_5min):
    """ Convert image from volume to flux. Suitable for mapping over IC.
    -------------------------------------------------------------------------------
    
    Args:
        image_volume_millionm3_5min (ee.Image) : image with volumetric data.
    
    Returns:
        image_flux_m_5min (ee.Image) : image with flux data.
    
    """
    
    old_unit = "millionm3"
    new_unit = "m"
    update_properties = ["unit","parameter","file_name"]
       
    image_global_area_m2_5min = ee.Image(GLOBAL_AREA_M2_5MIN_ASSET_ID)
    image_volume_m3_5min = image_volume_millionm3_5min.multiply(1e6)
    image_flux_m_5min = image_volume_m3_5min.divide(image_global_area_m2_5min)
    image_flux_m_5min = image_flux_m_5min.copyProperties(image_volume_millionm3_5min,exclude=update_properties)
    
    for update_property in update_properties:
        old_property = ee.String(image_volume_millionm3_5min.get(update_property))
        new_property = old_property.replace(old_unit,new_unit)
        image_flux_m_5min = image_flux_m_5min.set(update_property,new_property)
      
    return(ee.Image(image_flux_m_5min))

def flux_to_volume_5min_m3second_millionm3(image_flux_m3second_5min,temporal_resolution,year,month):
    """ Converts an image from flux (m3second) to volume
    -------------------------------------------------------------------------------
    Temporal resolution with year and month are required to convert m3second to 
    millionm3 per month or year. 
    
    
    Args:
        image_flux_m3second_5min (ee.Image) : image with flux data.
        temporal_resolution (string) : temporal resolution. Supported are year and
            month.
        year (integer) : year.
        month (integer) : month.
    
    Returns:
    
    """
    old_unit = "m3second"
    new_unit = "millionm3"
    update_properties = ["unit","parameter","file_name"]
    seconds_per_day = 86400
    
    if temporal_resolution == "month":
        seconds_per_month = monthrange(year,month)[1]*(seconds_per_day)
        image_volume_millionm3_5min = image_flux_m3second_5min.multiply(seconds_per_month).divide(1e6)
    elif temporal_resolution == "year":
        days_per_year = 366 if isleap(year) else 365
        seconds_per_year = days_per_year*(seconds_per_day)
        image_volume_millionm3_5min = image_flux_m3second_5min.multiply(seconds_per_year).divide(1e6)
    
    image_volume_millionm3_5min = image_volume_millionm3_5min.copyProperties(image_flux_m3second_5min,exclude=update_properties)
    
    for update_property in update_properties:
        old_property = ee.String(image_flux_m3second_5min.get(update_property))
        new_property = old_property.replace(old_unit,new_unit)
        image_volume_millionm3_5min = image_volume_millionm3_5min.set(update_property,new_property)
    return image_volume_millionm3_5min

@retry(wait_exponential_multiplier=10000, wait_exponential_max=100000)
def export_image_global_5min(image,description,output_asset_id):
    """ Exports an image using the 5min dimension.
    -------------------------------------------------------------------------------
    Starts an export task. Map function client side.
    
    Args:
        image (ee.Image) : earth engine image to export.
    
    Returns:
        None
     
    """
    
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = description,
        assetId = output_asset_id,
        dimensions = dimensions_5min,
        crs = CRS,
        crsTransform = CRS_TRANSFORM_5MIN,
        maxPixels = 1e10     
    )
    task.start()
    


def asset_exists(asset_id):
    """ Check if asset exists.
    -------------------------------------------------------------------------------
    Args:
        asset_id (string) : asset_id
        
    Returns:
        exists (boolean) : Returns 1 if asset exists, 0 otherwise.
    """
    command = "earthengine asset info {}".format(asset_id)
    try:
        result = subprocess.check_output(command,shell=True)
        exists = 1
    except:
        exists = 0
    return exists
    

def get_df_from_ic(ic_input_asset_id):
    """Create a pandas dataframe with the image asset ids from an imagecollection.
    -------------------------------------------------------------------------------
    
    Args:
        ic_input_asset_id (string) : asset id of input imagecollection.
        
    Returns:
        df (pandas dataframe) : Pandas dataframe with asset_ids.
    
    
    """
    command = "earthengine ls {}".format(ic_input_asset_id)
    asset_list_bytes = subprocess.check_output(command,shell=True).splitlines()
    asset_list =[x.decode("utf-8")  for x in asset_list_bytes]
    
    df = pd.DataFrame(asset_list)
    df.columns = ["input_image_asset_id"]
    df["input_ic_asset_id"] = ic_input_asset_id
    return df

def add_export_parameters_for_export(df,old_unit,new_unit,output_version):
    """ Add output_ic_asset_id, output_image_asset_id and description to df.
    -------------------------------------------------------------------------------
    
    Args:
        df (pandas dataframe) : Pandas dataframe with input i/ic asset ids.
        old_unit (regex) : old unit to replace in regex format. 
        new_unit (string) : new unit in string format.
        output_version (integer) : output version will be stored in description.
    
    
    """
    df_out = df.copy()
    df_out["output_ic_asset_id"] = df_out["input_ic_asset_id"].apply(lambda x: re.sub(old_unit,new_unit,x))
    df_out["output_image_asset_id"] = df_out["input_image_asset_id"].apply(lambda x: re.sub(old_unit,new_unit,x))
    df_out["description"] = df_out["output_image_asset_id"].apply(lambda x: (re.split("/",x)[-1])+"_V{:02.0f}".format(output_version)) 
    return df_out


def get_global_geometry(test=False):
    """ Returns a global or samplle geometry.
    -------------------------------------------------------------------------------
    please note that some python functions require you to modify the output.
    geometry.getInfo()['coordinates'] 
    
    This will likely change in a future version of earthengine.
    
    
    Args:
        test (boolean) : toggle testing mode.
        
    Returns:
        geomtery (ee.geometry) : Server side geometry object.
    
    """
    
    if test:
        geometry = ee.Geometry.Polygon(coords=[[-10.0, -10.0], [10,  -10.0], [10, 10], [-10,10]], proj= ee.Projection('EPSG:4326'),geodesic=False )
    else:  
        geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )
    return geometry

def get_crs_transform(spatial_resolution,test=False):
    if test:
        print("todo, add test crs")
        crs_transform = "todo, see function"
    else:
        if spatial_resolution == "5min":
            crs_transform = CRS_TRANSFORM_5MIN
        elif spatial_resolution == "30s":
            crs_transform = CRS_TRANSFORM_30S
    return crs_transform        

def get_dimensions(spatial_resolution):
    if spatial_resolution == "5min":
        dimensions = "{}x{}".format(X_DIMENSION_5MIN, Y_DIMENSION_5MIN)
    elif spatial_resolution == "30s":
        dimensions = "{}x{}".format(X_DIMENSION_30S, Y_DIMENSION_30S)
    else:
        raise Exception("Provide a valid spatial resolution, options include '5min' and '30s'")
    return dimensions

def get_grouped_reducer(reducer_name):
    """ Deprecated?"""
    if reducer_name == "sum":
        reducer = ee.Reducer.sum().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")
    elif reducer_name == "mean":
        reducer = ee.Reducer.mean().combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName= "zones")
    else:
        reducer = -9999
        raise "Reducer not supported"
    return reducer


def ensure_default_properties(obj):
    """ helper function for zonal stats"""
    obj = ee.Dictionary(obj)
    default_properties = ee.Dictionary({"mean": -9999,"count": -9999,"max":-9999})
    return default_properties.combine(obj)

def map_list(results, key):
    """ helper function for zonal stats"""
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
            geometry can be server side.
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
        geometry = geometry,
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
    
    
    i_count = ee.Image(i_zones).remap(zone_list, count_list).select(["remapped"],["count"])
    i_stat = ee.Image(i_zones).remap(zone_list, stat_list).select(["remapped"],[statistic_type])
    
    stat_properties = {"reducer":statistic_type,
                       "spatial_resolution":"30sPfaf06",
                       "zones":i_zones.get("file_name")
                      }
    count_properties = {"unit":"dimensionless",
                        "reducer":"count",
                        "spatial_resolution":"30sPfaf06",
                        "zones":i_zones.get("file_name")
                       }
    i_stat = i_stat.set(stat_properties) 
    i_count = i_count.set(count_properties)
    return i_stat , i_count