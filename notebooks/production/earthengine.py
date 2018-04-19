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
    