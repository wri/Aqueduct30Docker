
# coding: utf-8

# In[ ]:

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

