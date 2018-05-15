
# coding: utf-8

# In[1]:

""" Calculate sum of sinks at 5min zones.
-------------------------------------------------------------------------------

If a sub-basin contains one or more sinks (coastal and endorheic), the sum 
of riverdischarge at those sinks will be used. If a subbasin does not contain
any sinks or is too small to be represented at 5min, the main channel 
riverdischarge (30s validfa_mask) will be used. 

Creates a table with 5min zones and sum of sinks. Export to pandas dataframe
and featurecollection. 

Args:

"""


TESTING = 0
SCRIPT_NAME = "Y2018M05D15_RH_Sum_Sinks_5min_EE_V01"
OUTPUT_VERSION = 2

ZONES5MIN_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_5min_V04"
LDD_EE_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/global_lddsound_numpad_05min"
ENDOSINKS_EE_ASSET_ID = "projects/WRI-Aquaduct/Y2017M08D02_RH_Ingest_Aux_Rasters_GCS_EE_V02/output_V06/global_outletendorheicbasins_boolean_05min"

EXTRA_PROPERTIES = {"nodata_value":-9999,
                    "ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}

ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Output ee: " +  ee_output_path,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path)


# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

import pandas as pd
import numpy as np
import ee
import aqueduct3
ee.Initialize()


# In[5]:

i_hybas_lev06_v1c_merged_fiona_5min = ee.Image(ZONES5MIN_EE_ASSET_ID)
i_ldd_5min = ee.Image(LDD_EE_ASSET_ID)
i_endosinks_5min = ee.Image(ENDOSINKS_EE_ASSET_ID)


# In[6]:

i_sinks_5min =  i_ldd_5min.eq(5)
i_sinks_5min = i_sinks_5min.copyProperties(i_ldd_5min)
i_sinks_5min = i_sinks_5min.set("unit","boolean")


# In[7]:

# Geospatial constants
spatial_resolution = "5min"
geometry_server_side = aqueduct3.earthengine.get_global_geometry(test=TESTING)
geometry_client_side = geometry_server_side.getInfo()['coordinates']
crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)


# In[8]:

def post_process_results(result_list,function_properties,extra_properties=EXTRA_PROPERTIES):
    """Client side function to convert results of reduceRegion to pandas dataframe.
    -------------------------------------------------------------------------------
    
    Adds additional properties. The script is client side for convenience reasons.
    A more robust and fast approach would be to add the extra_properties to the 
    server side dictionary.
    
    Args:
        result_list (ee.List) : List of dictionaries. Result from reduceRegion
        function_properties (dictionary) : Additional properties used in the 
            reduceRegion function call.
        extra_properties (dictionary) : Additional properties set at global level. 
    
    Returns:
        df (pd.DataFrame) : Pandas dataframe with extra properties.
    
    
    """
    extra_properties = {**function_properties, **EXTRA_PROPERTIES}
    result_list_clientside = result_list.getInfo()
    df = pd.DataFrame(result_list_clientside)
    df = df.assign(**extra_properties)
    df = df.apply(pd.to_numeric, errors='ignore')
    return df  





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
    
    return result_list,i_result, i_count



# In[9]:

output_dict = {}
result_list, output_dict["global_sum_sinks_dimensionless_5minPfaf06"], output_dict["global_count_sinks_dimensionless_5minPfaf06"] = master(i_zones = i_hybas_lev06_v1c_merged_fiona_5min,
                                                                                                                                                   i_values = i_sinks_5min,
                                                                                                                                                   geometry = geometry_client_side,
                                                                                                                                                   crs_transform = crs_transform,
                                                                                                                                                   statistic_type = "sum",
                                                                                                                                                   extra_properties= {})




# In[10]:

# Export to image

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


# In[11]:

# Export to pandas dataframe and CSV

output_file_name = "global_sum_sinks_dimensionless_5minPfaf06"
output_file_path_pkl = "{}/{}.pkl".format(ec2_output_path,output_file_name)
output_file_path_csv = "{}/{}.csv".format(ec2_output_path,output_file_name)

df = pd.DataFrame(result_list.getInfo())

df2 = df.copy()
df2["zones"] = df["zones"].astype(np.int64)
df2["sum"] = df["sum"].astype(np.int64)
df2.to_pickle(output_file_path_pkl)
df2.to_csv(output_file_path_csv)


# In[12]:

# Export as FeatureCollection
sample_geometry = ee.Geometry.Point(1,1)
fc = ee.FeatureCollection(result_list.map(lambda d: ee.Feature(sample_geometry,d)))
fc = fc.setMulti(EXTRA_PROPERTIES)
fc = fc.copyProperties(output_dict["global_sum_sinks_dimensionless_5minPfaf06"])
    
taskParams = {'json' : fc.serialize(), 'type': 'EXPORT_FEATURES', 'assetId': 'users/rutgerhofste/fcexporttest','description': 'adescription'}
taskId = ee.data.newTaskId()[0]
ee.data.startProcessing(taskId, taskParams)



# In[14]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[13]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:00:14.735956
# 

# In[ ]:



