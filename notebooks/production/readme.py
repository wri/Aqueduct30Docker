
# coding: utf-8

# # Steps for Aqueduct 30
# 
# This document will keep track of what scripts to run to get to the resulst
# 
# 
# ### Main Branch
# 
# 1.  **Y2017M07D31_RH_copy_S3raw_s3process_V01.ipynb**  
#     Copy files from raw data folder to process data folder, all within S3. 
# 1.  **Y2017M07D31_RH_download_PCRGlobWB_data_V01.ipynb**  
#     Download the data to your machine, unzip files
# 1.  **Y2017M07D31_RH_Convert_NetCDF_Geotiff_V01**  
#     convert netCDF4 to Geotiff
# 1.  **Y2017M08D02_RH_Upload_to_GoogleCS_V01**  
#     upload files to Google Cloud Storage. 
# 1. **Y2017M08D02_RH_Ingest_GCS_EE_V01**  
#     ingest data from Google Cloud Storage to EarthEngine, adding metadata
# 1. **Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01**  
#     A couple of indicators are shared in ASCII format. Converting to geotiff
# 1. **Y2017M08D02_RH_Merge_HydroBasins_V01**  
#     copy hydrobasin files from S3 and merge in pyhton using Fiona
# 1. **Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01**  
#     This script ingests some auxiliary datafile onto earth engine included DEM, StreamFlow Network etc. 
# 1. **Y2017M08D22_RH_UpstreamDowstream_V01**  
#     add upstream and downstream PFAFIDs to the merged hydrobasin shp/csv file. 
# 
# ### Groundwater Branch
# 
# ### Flood Risk Branch
# 
# ### Country Shapefile Branch
# 
# ### Other Indicators Branch
#     
#     
# 

# In[ ]:



