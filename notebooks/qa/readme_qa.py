
# coding: utf-8

# ### QA Steps for Aqueduct 
# 
# 1. **Y2018M06D07_RH_QA_Create_Users_PostGIS_V01**  
#     Create users and manage permissions in database.
# 
# 
# 1. **Y2018M06D05_RH_QA_Sample_Raster_Hydrobasins_V01**  
#     Create rasterized zones at 30s and 5min resolution.
# 1. **Y2018M05D02_RH_Convert_Area_Raster_EE_GCS_S3_V01**  
#     Area rasters were created in ee, storing in gcs.
# 1. **Y2018M06D04_RH_QA_download_sample_geotiffs_5min_V01**  
#     Store sample demand and riverdischarge geotiffs in S3 folder. 
# 1. **Y2018M06D04_RH_QA_Download_sample_dataframes_riverdischarge_V01**  
#     Download pickled dataframes and convert to csv of combined riverdischarge. 
# 1. **Y2018M06D05_Rh_QA_Zonal_Stats_Demand_EE_V01**  
#     Check output of zonal stats demand. Convert to CSV.
# 1. **Y2018M05D15_RH_QA_Sum_Sinks_5min_EE_V01**  
#     Download pickled dataframes and convert to csv of sum sinks. 
# 1. **Y2018M06D04_RH_QA_ptot_riverdischarge_PostGIS_V01**  
#     Create a table with sample data from the ptot and riverdischarge results.
# 1. **Y2018M06D05_RH_QA_Total_Demand_PostGIS_30sPfaf06_V01**  
#     For a specific sub basin, query a 10y series for demand and supply to check input for ma10.
# 1. **Y2018M06D04_RH_QA_ma10_results_PostGIS_V02**  
#     Create a table with sample data from the ma 10 results.
# 
# 
# ## Comparing AQ2.1
#     
# 1. **Y2018M06D05_RH_QA_Aqueduct21_Flux_Shapefile_V01**  
#     Create Aqueduct 2.1 shapefile with fluxes. Store on S3 and GCS.
# 1. **Y2018M06D11_RH_QA_Ingest_Aq21_Flux_Shapefile_V01**  
#     Ingest aqueduct 2.1 shapefile with fluxes into earthengine.
# 1. **Y2018M06D19_RH_QA_Download_Aq21projection_Shapefile_V01**
#     Download Aqueduct 2.1 projection shapefile and upload to S3 and GCS.
# 1. **Y2018M06D19_RH_QA_Ingest_Aq21projection_Shapefile_V01**  
#     Ingest aqueduct 2.1 projection shapefile with fluxes into earthengine.    
# 1. **Y2018M06D18_RH_QA_IngestAq30_Shapefile_V01**   (Move to main repo?)  
#     Ingest aqueduct 3.0 shapefile into earthengine. 
# 1. **Y2018M06D08_RH_QA_Aqueduct21_Demand_Ingest_GCS_EE_V01**  
#     Convert Aqueduct 2.1 demand to geotiff. 
# 1. **Y2018M06D18_RH_QA_AQ21_AQ30_Demand_Zonal_Stats_EE_V01**  
#     Zonal statistics with Aq21 basins as zones and AQ 2.1 and AQ 3.0 as values. 
# 1. **Y2018M06D19_RH_QA_AQ21_AQ30_Demand_Cleanup_V01**  
#     Combine zonal statistics of different indicators and calculate flux.
#  
# 

# In[ ]:



