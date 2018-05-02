
# coding: utf-8

# In[1]:

""" Area rasters were created in ee, storing in gcs and s3.
-------------------------------------------------------------------------------
Copy area rasters (5min and 30s) to Google Cloud Storage.

Creates global rasters by default and sample rasters in testing mode. 
Sample rasters have dimensions as specified in aqueduct3.earthengine.

The global raster at 30s will be split up in two parts.


Author: Rutger Hofste
Date: 20180502
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTTING (boolean) : Toogle testing mode. 
    SCRIPT_NAME (string) : Script name.
    EE_INPUT_PATH (string) : earthengine input path.
    INPUT_FILE_NAMES (list) : List of strings with file_names to export.
    OUTPUT_GCS_BUCKET (string) : Google Cloud Storage bucket.
    OUTPUT_VERSION (integer) : output version.     

Returns:


"""
TESTING = 1
SCRIPT_NAME = "Y2018M05D02_RH_Convert_Area_Raster_EE_GCS_S3_V01"
EE_INPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/"
INPUT_FILE_NAMES = [ "global_area_m2_5min_V05",
                     "global_area_m2_30s_V05"]
OUTPUT_GCS_BUCKET = "aqueduct30_v01"
OUTPUT_VERSION = 5

# ETL
gcs_output_path = "gs://{}/{}/output_V{:02.0f}/".format(OUTPUT_GCS_BUCKET,SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ee : " +  EE_INPUT_PATH +
      "\nOutput s3 : " + s3_output_path +
      "\nOutput gcs : " + gcs_output_path)


# In[2]:

import time, datetime, sys
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


def main():
    for file_name in INPUT_FILE_NAMES:
        input_asset_id = EE_INPUT_PATH + file_name
        if file_name == "global_area_m2_5min_V05":
            spatial_resolution =  "5min"
            if TESTING:
                output_file_name = "sample_area_m2_5min_V05"
            else:
                output_file_name = "global_area_m2_5min_V05"
        elif file_name == "global_area_m2_30s_V05":
            spatial_resolution =  "30s"
            if TESTING:
                output_file_name = "sample_area_m2_30s_V05"
            else:
                output_file_name = "global_area_m2_30s_V05"
        else:
            raise Exception("File Name not recognized")

        output_file_name_prefix = "{}/output_v{:02.0f}/{}".format(SCRIPT_NAME,OUTPUT_VERSION,output_file_name)
        print(output_file_name_prefix)
        crs_transform = aqueduct3.earthengine.get_crs_transform(spatial_resolution)
        dimensions = aqueduct3.earthengine.get_dimensions(spatial_resolution)
        geometry = aqueduct3.earthengine.get_global_geometry(test=TESTING)
        # Weird legacy stuff: 
        # https://groups.google.com/d/msg/google-earth-engine-developers/TViMuO3ObeM/cpNNg-eMDAAJ
        geometry_client_side = geometry.getInfo()['coordinates']
        crs = aqueduct3.earthengine.CRS

        image = ee.Image(input_asset_id)
        task = ee.batch.Export.image.toCloudStorage(
                    image= image, 
                    description= file_name,
                    bucket = OUTPUT_GCS_BUCKET,
                    fileNamePrefix = output_file_name_prefix,
                    region = geometry_client_side,
                    crs = crs,
                    crsTransform = crs_transform,
                    maxPixels = 1e10)
        task.start()

if __name__ == "__main__":
    main()


# In[4]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:06.296458
# 
