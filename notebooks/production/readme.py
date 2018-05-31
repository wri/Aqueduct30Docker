
# coding: utf-8

# # Steps for Aqueduct 30
# 
# This document will keep track of what scripts to run to get to the results. 
# 
# ### Setup Checks  
# 1. **Y2018M04D16_RH_Checks_V01**  
#    Run setup checks for credentials. 
# 
# 
# 
# ### PostGIS PostgreSQL database
# 1. **Y2017M11D15_RH_Create_PostGIS_Database_V01**  
#     create postgis database using AWS RDS.  
# 
# 
# ### HydroBasins and FAO names
#     
# 1. **Y2017M08D02_RH_Merge_HydroBasins_V02**  
#     copy hydrobasin files from S3 and merge in python using Fiona. 
# 1. **Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01**  
#     Ingest hydrobasin rasters in earthengine.
# 1. **Y2017M08D22_RH_Upstream_V01**  
#     Add upstream PFAFIDs to the merged hydrobasin shp/csv file. 
# 1. **Y2017M08D23_RH_Downstream_V01**  
#     Add downstream PFAFIDs to the merged hydrobasin csv file.
# 1. **Y2017M08D23_RH_Merge_FAONames_V01**  
#     Merge the FAO shapefiles into one Shapefile (UTF-8).
# 1. **Y2017M08D23_RH_Buffer_FAONames_V01**  
#     Create a negative buffer for the FAO basins to avoid sliver polygons.
# 1. **Y2017M08D25_RH_spatial_join_FAONames_V01**  
#     Add the FAO Names to the HydroBasins shapefile.
# 1. **Y2017M08D29_RH_Merge_FAONames_Upstream_V01** (Archive!, uses old list based database format.)
#     Join the tables with the FAO names and the upstream / downstream relations.
#     
# 1. **Y2017M11D15_RH_Add_HydroBasins_postGIS_V01**  
#     Add hydrobasins geometry and table to postGIS database.    
# 1. **Y2017M11D22_RH_FAO_To_Database_V01**    
#     Store FAO data in AWS RDS database.
# 1. **Y2017M11D23_RH_Upstream_Downstream_Basin_To_Database_V01**  
#     Add upstream pfaf ID to postgis database.  
# 
# 
# ### Group Delta Region in HydroBasin level 6
# Delta basins grouped manually 
# 1. **Y2018M03D09_RH_Delta_Regions_Manual_V01**  
#     Several manual steps to create a list of delta sub-basins.
# 1. **Y2018M02D15_RH_GDBD_Merge_V01**  
#     This script will reproject GDBD basins and streams and merge them.
# 1. **Y2018M02D16_RH_Number_Streams_Per_Basin_V01**  
#     Determine the number of streams per GDBD basin. 
#     
# 
# 
#     
# ### PCRGlobWB 2.0
# 
# 1.  **Y2017M07D31_RH_copy_S3raw_s3process_V02**  
#     Copy files from raw data folder to process data folder, all within S3. 
# 1.  **Y2017M07D31_RH_Download_PCRGlobWB_Data_V02**  
#     Download the data to your machine, unzip files.
# 1.  **Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02**  
#     Convert netCDF4 to Geotiff. Store on GCS.    
# 1. **Y2017M08D02_RH_Ingest_GCS_EE_V02**  
#     Ingest PCRGLOBWB timeseries data on Google Earth Engine
# 1. **Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V02**  
#     A couple of indicators are shared in ASCII format. Converting to geotiff.Store on GCS.
# 1. **Y2018M04D12_RH_Ingest_Indicators_GCS_EE_V01**  
#     Ingest the non-time-series indicators into earth engine. 
# 1. **Y2018M04D18_RH_Convert_Aux_Rasters_Geotiff_V01**  
#     Convert auxiliary files like DEM, LDD etc. to geotiff. Store on GCS.
# 1. **Y2017M08D02_RH_Ingest_Aux_Rasters_GCS_EE_V02**  
#     Ingest additional rasters like DEM, LDD etc.
# 1. **Y2017M09D05_RH_Create_Area_Image_EE_V01**  
#     Create an image with the pixel area size in m2 to go from flux to volumen and vice versa.
# 1. **Y2018M04D20_RH_Zonal_Stats_Area_EE_V01**  
#     Zonal statistics for basin area. Export in table format.
# 1. **Y2018M05D22_RH_Store_Area_PostGIS_30sPfaf06_V01**  
#     Store area of 30sPfaf06 basins in postGIS database. 
# 1. **Y2018M04D18_RH_Demand_Fluxes_5min_EE_V01**  
#     Demand data is provided as volumes. Calculate Fluxes.
# 1. **Y2018M04D22_RH_Zonal_Stats_Demand_EE_V01**  
#     Zonal statistics for basin demand. ExportArea rasters were created in ee, storing in gcs and s3.t in table format.
#     
# 1. **Y2018M05D21_RH_Store_Demand_PostGIS_30sPfaf06_V01**  (might get archived)
#     Store demand data in postGIS database.    
#     
# 1. **Y2018M04D19_RH_Supply_Discharge_Volume_5min_EE_V01**  
#     Supply and discharge data is provided as m3second. Convert to millionm3.
# 1. **Y2018M05D01_RH_Supply_Fluxes_5min_EE_V01**  
#     Supply data is available in m3second and millionm3. Convert to flux. 
# 1. **Y2018M05D02_RH_Prepare_Mask_Discharge_Pixels_V01**  
#     Create area and streamorder images that can be used to mask riverdischarge.
# 1. **Y2018M05D03_RH_Mask_Discharge_Pixels_V01**  
#     Mask pixels based on area and streamorder.
#     
# 1. **Y2018M05D28_RH_Riverdischarge_Mainchannel_30sPfaf06_EE_V01** (client side, pick 1)  
#     Apply the combined mask and calculate the max discharge per 30spfaf06 zone.  
# 1. **Y2018M05D28_RH_Riverdischarge_Mainchannel_30sPfaf06_EE_V02** (Server side, pick 1)  
#     Apply the combined mask and calculate the max discharge per 30spfaf06 zone.
# 
# 
# 1. **Y2018M05D15_RH_Sum_Sinks_5min_EE_V01**  
#     Calculate sum of sinks at 5min zones.
# 1. **Y2018M05D15_RH_Sum_Riverdischarge_Sinks_5min_EE_V01**  
#     Sum riverdischarge at sinks at 5min resolution. 
# 1. **Y2018M05D16_RH_Final_Riverdischarge_30sPfaf06_V02**  
#     Combine riverdischarge in main channel and sinks.     
# 1. **Y2018M05D17_RH_Store_Riverdischarge_PostGIS_30sPfaf06_V02**  (might get archived)  
#     Store combined riverdischarge data in postGIS database.     
# 1. **Y2018M05D23_RH_Simplify_DataFrames_Pandas_30sPfaf06_V03**  
#     Combine and simplify demand and riverdischarge dataframes.
# 1. **Y2018M05D24_RH_Ingest_Simplified_Dataframes_PostGIS_30sPfaf06_V01**   
#     Store merged and simplified pandas dataframes in postGIS database. 
# 1. **Y2018M05D29_RH_Total_Demand_PostGIS_30sPfaf06_V01**  
#     Create total WW and total WN columns in simplified table.
#      
# 
#     
#   
# 
#     
# 
# ### Groundwater Branch
# 
# ### Flood Risk Branch
# 
# ### Country Shapefile Branch
# 
# ### Other Indicators Branch
#     
# ### Aux Files  
# 
# 1. **Y2017M10D09_RH_create_Line_Shape_File_V01**
#     Create a shapefile to visualize the flow network
# 1. **Y2018M04D24_RH_Create_Additional_Pcrasters_V01**  
#     Use pcraster to create additional rasters such as streamorder.
# 1. **Y2018M04D25_RH_Convert_Pcrasters_Map_Geotiff_V01**  
#     Convert pcrasters to geotiffs. 
# 1. **Y2018M04D25_RH_Ingest_Pcraster_GCS_EE_V01**  
#     Ingest pcraster generated files from Cloud Storage to Earth Engine.
#     
# 
# ### Create Final Databases
# 
# 1. **Y2017M11D16_RH_Add_Water_Stress_postGIS_V01 #NOT USED**    
#     add pcrGlob water demand, supply and stress data to database.
#     
# 
# 
# ### Evaporative Stress Index  
# 1.  **Y2018M03D22_RH_Download_ESI_V01**  
#     Download ESI (Evaporative Stress Index) to Instance, Google Cloud Services and Amazon S3.
#     
#     
# ### QA
# 1.  **Y2018M05D02_RH_Convert_Area_Raster_EE_GCS_S3_V01**
#     Area rasters were created in ee, storing in gcs and s3.
# 
#     
# 
# 

# In[ ]:



