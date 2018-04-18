
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
# 1. **Y2017M08D02_RH_Merge_HydroBasins_V01**  
#     copy hydrobasin files from S3 and merge in pyhton using Fiona. It ingests the data into ee. 
# 1. **Y2017M08D22_RH_Upstream_V01**  
#     add upstream PFAFIDs to the merged hydrobasin shp/csv file. 
# 1. **Y2017M08D23_RH_Downstream_V01**  
#     add downstream PFAFIDs to he merged hydrobasin csv file.
# 1. **Y2017M08D23_RH_Merge_FAONames_V01**  
#     merge the FAO shapefiles into one Shapefile (UTF-8) and also rasterize using gdal.
# 1. **Y2017M08D23_RH_Buffer_FAONames_V01**  
#     Create a negative buffer for the FAO basins to avoid sliver polygons.
# 1. **Y2017M08D25_RH_spatial_join_FAONames_V01**  
#     Add the FAO Names to the HydroBasins shapefile.
# 1. **Y2017M08D29_RH_Merge_FAONames_Upstream_V01**  
#     join the tables with the FAO names and the upstream / downstream relations.
# 1. **Y2017M11D15_RH_Add_HydroBasins_postGIS_V01**  
#     Add hdyrobasins geometry and table to postGIS database.    
# 1. **Y2017M11D22_RH_FAO_To_Database_V01**    
#     Store FAO data in AWS RDS database.
# 1. **Y2017M11D23_RH_Upstream_Downstream_Basin_To_Database_V01**  
#     add upstream pfaf ID to postgis database.  
# 
# 
# ### Group Delta Region in HydroBasin level 6
# Delta basins grouped manually 
# 1. **Y2018M03D09_RH_Delta_Regions_Manual_V01**  
#     several manual steps to create a list of delta sub-basins.
# 1. **Y2018M02D15_RH_GDBD_Merge_V01**  
#     This script will reproject GDBD basins and streams and merge them.
# 1. **Y2018M02D16_RH_Number_Streams_Per_Basin_V01**  
#     determine the number of streams per GDBD basin. 
#     
# 
# 
#     
# ### PCRGlobWB 2.0
# 
# 1.  **Y2017M07D31_RH_copy_S3raw_s3process_V02**  
#     Copy files from raw data folder to process data folder, all within S3. 
# 1.  **Y2017M07D31_RH_download_PCRGlobWB_data_V02**  
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
# 1. **Y2018M04D18_RH_Standardize_Demand_Units_5min_EE_V01**  
#     Demand data is provided as volumes. Calculate Fluxes.
#     
#     
#     
#     
#     
# 1. **Y2017M09D05_RH_create_area_image_EE_V01**  
#     Create an image with the pixel area size in m2 o go from flux to volumen and vice versa.   
#     
#     November 24th 2017, restructure data for better processing in postGIS vertical table. Absolute values, Short term Trend, Short term average, Long term Trend and Long term average for all demand indicators, runoff and discharge and water stress uncapped.  
#     
# 1. **Y2017M11D24_RH_Prepare_Image_Collections_EE_V01**  
#     put all earth engine imagecollections in the same format (millionm^3  and dimensionless).
# 1. **Y2017M11D29_RH_totalWW_totalWN_WS_Pixel_EE_V01**  
#     calculate total demand WW WN and water stress at pixel level.   
# 1. **Y2017M12D06_RH_Conservative_Basin_Sinks_EE_V01**  
#     find conservative discharge point.    
# 1. **Y2017M12D01_RH_ZonalStats_PCRGlobWB_toImage_EE_V01**  
#     calculate sectoral demand, total demand, runoff and discharge per Hydrobasin level 6, export to imageCollections.
# 1. **Y2017M12D07_RH_ZonalStats_MaxQ_toImage_EE_V01**  
#     find conservative and global max discharge value per Hydrobasin level 6 and export to imageCollection. 
# 1. **Y2017M12D12_RH_Zonal_Areas_EE_V01**  
#     create raster images with the area per basin at 5min and 30s resolution.
# 1. **Y2018M02D27_RH_Moving_Average_Demand_EE_V01**  
#     moving average for demand at basin resolution.
# 1. **Y2018M03D01_RH_Moving_Average_Discharge_EE_V01**  
#     moving average for discharge at basin resolution.
# 1. **Y2018M03D24_RH_Moving_Average_Demand_To_GCS_V01**  
#     store moving average results for demand in a CSV file in GCS  
# 1. **Y2018M03D26_RH_Moving_Average_Discharge_To_GCS_V01**  
#     store moving average results for discharge in a CSV file in GCS 
#     
#     
# 
#     
# February 27th 2018, based on a discusssion at the Utrecht University I will have to take a slightly different approach. 
#     This includes the grouping of delta basins and calculating a running average. I will put the unused scripts in an archive folder. I also cleaned up the scripts and assets in GEE 
# 
#     Archived (unused) scripts:  
# 
#     
# 1. **Y2017M12D12_RH_Water_Stress_EE_V02**  
#     Calculate water stress using maximum discharge and total withdrawals per month or year. 
# 1. **Y2017M12D15_RH_Statistics_Trend_EE_V01**  
#     Calculate short term and long term average and 2014_trend for Q, WW and WN per basin.
# 1. **Y2017M12D19_RH_Water_Stress_Reduced_EE_V01**  
#     Calculate water stress using reduced (Long/Short Mean/Trend) maximum discharge and total withdrawals. 
# 1. **Y2017M12D20_RH_Water_Stress_Year_From_Month_EE_V01**  
#     Calculate annual water stress based on average of all months.
# 1. **Y2017M12D28_RH_zonal_stats_EE_toGCS_V02**  
#     Calculate zonal statistics for EE images and HydroBasin level 6 zones. Export to GCS. 
#     
# 1. **Y2017M12D28_RH_EE_Results_To_PostGIS_V01**  
#     Store earth engine raster results in postgis. 
#     
#     
#     
#     Archived (unused) scripts:  
#     
# 1.  **Y2017M08D02_RH_Upload_to_GoogleCS_V02**(obsolete as of 20180416)  
#     Upload files to Google Cloud Storage. 
#     
# 1. **Y2017M08D30_RH_Average_Supply_EE_V01**  
#     This script will canculate the average PCRGlobWB2.0 supply (local runoff) using the ee python API.
# 1. **Y2017M09D01_RH_linear_trend_Ag_Demand_EE_V01**  
#     Due to the sensitivity of the model to irrigation demand we take the linear trend of 2004 - 2014 for ag demand.
# 
# 1. **Y2017M09D11_RH_zonal_stats_EE_V01**  
#     Calculate zonal statistics for EE images and HydroBasin level 6 zones. Export to GCS. 
# 1. **Y2017M09D14_RH_merge_EE_results_V01**  
#     This script will merge the csv files into one big file/dataFrame.    
# 1. **Y2017M09D15_RH_Add_Basin_Data_V01**  
#     add data from upstream, downstream and basin to dataframe.
# 1. **Y2017M10D02_RH_Calculate_Water_Stress_V01**  
#     calculate total demand (Dom, IrrLinear, Liv, Ind) and Reduced Runoff and water stress.  
# 1. **Y2017M10D04_RH_Threshold_WaterStress_V02**  
#     set arid and low water use, add numerical and labeled thresholds. 
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
# 

# In[ ]:



