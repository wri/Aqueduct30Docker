
# coding: utf-8

# # Steps for Aqueduct 30
# 
# This document will keep track of what scripts to run to get to the results. 
# 
# 
# ## Abbreviations
# 
# 
# 1. Baseline Water Stress | WS
# 1. Baseline Water Depletion | WD
# 
# 
# ## Results at hydrobasin level 6
# 
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
# 1. **Y2017M11D22_RH_FAO_To_Database_V02**    
#     Store FAO data in AWS RDS database.
# 1. **Y2017M11D23_RH_Upstream_Downstream_Basin_To_Database_V01**  
#     Add upstream pfaf ID to postgis database.
# 1. **Y2018M07D03_RH_Upload_Hydrobasin_Mapbox_V01**  
#     Upload simplified hydrobasins to mapbox for visualization purposes.
# 1. **Y2018M07D18_RH_Upload_Hydrobasin_Carto_V01**  
#     Upload simplified hydrobasins to carto for visualization purposes.
#     
# 1. **Y2019M02D11_RH_River_Networks_30s_V01**  
#     Merge river networks per continent. Store in multiple formats.
# 
# ### Group Delta Region in HydroBasin level 6
# 
# 1. **Y2018M03D09_RH_Delta_Regions_Manual_V01**  
#     Several manual steps to create a list of delta sub-basins.
# 1. **Y2018M02D15_RH_GDBD_Merge_V01**  
#     This script will reproject GDBD basins and streams and merge them.
# 1. **Y2018M02D16_RH_Number_Streams_Per_Basin_V01**  encidi
#     Determine the number of streams per GDBD basin.
# 1. **Y2018M07D25_RH_Basin_Manual_Step_V01**  
#     Manual step to create hydrobasin equivalent of GDBD deltas. 
# 1. **Y2018M07D25_RH_Delta_Lookup_Table_PostGIS_V01**  
#     Store gdbd and hybas deltas in postgis in lookup table.
# 
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
# 1. **Y2018M05D21_RH_Store_Demand_PostGIS_30sPfaf06_V01**  (might get archived)
#     Store demand data in postGIS database.        
# 1. **Y2018M04D19_RH_Supply_Discharge_Volume_5min_EE_V01**  
#     Supply and discharge data is provided as m3second. Convert to millionm3.
# 1. **Y2018M05D01_RH_Supply_Fluxes_5min_EE_V01**  
#     Supply data is available in m3second and millionm3. Convert to flux. 
# 1. **Y2018M05D02_RH_Prepare_Mask_Discharge_Pixels_V01**  
#     Create area and streamorder images that can be used to mask riverdischarge.
# 1. **Y2018M05D03_RH_Mask_Discharge_Pixels_V01**  
#     Mask pixels based on area and streamorder.    
# 1. **Y2018M05D28_RH_Riverdischarge_Mainchannel_30sPfaf06_EE_V01** (client side, pick 1)  
#     Apply the combined mask and calculate the max discharge per 30spfaf06 zone.  
# 1. **Y2018M05D28_RH_Riverdischarge_Mainchannel_30sPfaf06_EE_V02** (Server side, pick 1)  
#     Apply the combined mask and calculate the max discharge per 30spfaf06 zone.
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
#     
# #### Individual Subbasins  
# this is where the workflow splits up. Delta subbasins are grouped and 
# handled in a separate branch. See section Grouped Delta Regions for
# these scripts.  
# 
# 1. **Y2018M05D29_RH_Total_Demand_PostGIS_30sPfaf06_V01**  
#     Create total WW and total WN columns in simplified table.
# 1. **Y2018M06D01_RH_Temporal_Reducers_PostGIS_30sPfaf06_V01**  
#     Determine moving average, min max and linear regression.
# 1. **Y2018M06D25_RH_Cap_Linear_Trends_PostGis_30sPfaf06_V01**  
#     Cap the results of the ols at the minmax of the moving window. 
# 1. **Y2018M06D04_RH_Arid_LowWaterUse_PostGIS_30sPfaf06_V02**  
#     Add column for arid, lowwateruse and aridandlowwateruse for each subbasin. 
# 
# 
# #### Water Stress and Depletion hydrobasin 6
# 
# 1. **Y2018M06D04_RH_Water_Stress_PostGIS_30sPfaf06_V02**  
#     Calculate water stress with raw, ma10 and ols10 at subbasin level.
# 1. **Y2018M06D28_RH_WS_Full_Range_Ols_PostGIS_30sPfaf06_V02**  
#     Fit linear trend and average on 1969-2014 timeseries of linear trends.
# 1. **Y2018M07D09_RH_Arid_LowWaterUse_Full_Ols_PostGIS_V01**  
#     Using the full range ols_ols10, apply the arid and lowwateruse thresholds.
# 1. **Y2018M07D09_RH_Apply_AridLowOnce_Mask_PostGIS_V01**  
#     Apply the mask for arid and lowwater use subbasins based on ols_ols10 (once).
# 1. **Y2018M07D12_RH_Annual_Scores_From_Months_PostGIS_V01**  
#     Calculate Annual Scores by averaging monthly values. 
# 1. **Y2018M07D12_RH_Merge_Simplify_Tables_PostGIS_V01**  
#     Merge and simplify master table and annual scores based on months. 
# 1. **Y2018M07D10_RH_Update_WaterStress_AridLowOnce_PostGIS_V01**  
#     Create columns for ws_r and ws_s with aridlowwateruse_once mask. 
# 1. **Y2018M07D12_RH_WS_Categorization_Label_PostGIS_V01**  
#     Add category and label for water stress. 
# 
#     
#     
# #### Water Stress and Depletion Deltas
# 
# 1. **Y2018M07D25_RH_Join_Deltas_Values_V01**  
#     Join delta_ids, supply and demand tables.    
# 1. **Y2018M07D25_RH_Group_Delta_Basins_V01**  
#     Group Delta basins and calculate supply and demand. 
# 1. **Y2018M07D26_RH_Deltas_Total_Demand_V01**  
#     Create total WW and total WN columns in simplified table for deltas.
# 1. **Y2018M07D26_RH_Deltas_Temporal_Reducers_V01**  
#     Determine moving average, min max and linear regression for deltas.
# 1. **Y2018M07D26_RH_Deltas_Cap_Linear_Trends_V01**  
#     Cap the results of the ols at the minmax of the moving window for deltas.
# 1. **Y2018M07D26_RH_Deltas_Arid_LowWaterUse_V02**  
#     Add column for arid, lowwateruse and aridandlowwateruse for each subbasin for deltas.  
# 1. **Y2018M07D26_RH_Deltas_Water_Stress_V01**  
#     Calculate water stress with raw, ma10 and ols10 at subbasin level for deltas.  
# 1. **Y2018M07D26_RH_Deltas_WS_Full_Range_Ols_V01**  
#     Fit linear trend and average on 1969-2014 timeseries of linear trends for deltas.
# 1. **Y2018M07D27_RH_Deltas_Arid_LowWaterUse_Full_Ols_V01**  
#     Using the full range ols_ols10, apply the arid and lowwateruse thresholds for deltas.
# 1. **Y2018M07D27_RH_Deltas_Apply_AridLowOnce_Mask_V01**  
#     Apply the mask for arid and lowwater use subbasins based on ols_ols10 (once) for deltas.
# 1. **Y2018M07D27_RH_Deltas_Annual_Scores_From_Months_V01**  
#     Calculate Annual Scores by averaging monthly values for deltas.
# 1. **Y2018M07D27_RH_Deltas_Merge_Simplify_Tables_V01**  
#     Merge and simplify master table and annual scores based on months for deltas. 
# 1. **Y2018M07D27_RH_Deltas_Update_WaterStress_AridLowOnce_V01**  
#     Create columns for ws_r and ws_s with aridlowwateruse_once mask for deltas. 
# 1. **Y2018M07D27_RH_Deltas_WS_Categorization_Label_V01**  
#     Add category and label for water stress for deltas. 
#   
# #### Water Stress and Depletion merge subbasins and deltas.
# 1. **Y2018M07D30_RH_Add_DeltaID_Subbasins_V01**  
#     Add delta id column to subbasin results. 
# 1. **Y2018M07D30_RH_Merge_Subbasins_Deltas_V01**  (rerun 20180808, version increase)  
#     Merge subbasin results and delta results.
# 1. **Y2018M07D30_RH_Coalesce_Columns_V01** (typo, rerunning 20180814 version increase)
#     Use first valid of delta or subbasin column. 
# 1. **Y2018M07D30_RH_Replace_Null_Deltas_V01**  
#     Replace Null values with numbers to prepare for bigquery. 
# 1. **Y2018M07D17_RH_RDS_To_S3_V02**  
#     Convert PostgreSQL RDS to CSV files on S3 and GCP.
# 1. **Y2018M07D30_RH_GCS_To_BQ_V01**  
#     Store Cloudstorage CSV files into bigquery table.
# 
# 
# #### Inter Annual Variability (between year)  
# 1. **Y2018M07D31_RH_Inter_Annual_Varibility_Average_STD_V01**  
#     Calculate inter annual variability.  
# 1. **Y2018M07D31_RH_Inter_Annual_Variability_Coef_Var_V01**  
#     Calculate coefficient of variation.
# 1. **Y2018M07D31_RH_Inter_AV_Cat_label_V01**  
#     Categorize and label inter annual variability.
# 
# #### Intra Annual Variability (within year) .
# 1. **Y2018M07D31_RH_Intra_Annual_Variability_Average_STD_V01**  
#     Calculate intra annual variability intermediate results.
# 1. **Y2018M08D01_RH_Intra_Annual_Variability_Coef_Var_V01**  
#     Calculate coefficient of variation. 
# 1. **Y2018M08D02_RH_Intra_Annual_Variability_Cat_Label_V01**  
#     Categorize and label intra annual variability.
#     
# 
# #### Groundwater
# 1. **Y2018M09D03_RH_GWS_Tables_To_BQ_V01**  
#     Ingest tabular results to BigQuery.
# 1. **Y2018M09D03_RH_GWS_Cat_Label_V01**  
#     Add category and Label for groundwater stress and trend.
# 
# #### Drought Severity (not used)
# 1. **Y2018M09D05_RH_DS_Zonal_Stats_V01**  
#     Zonal stats for drought severity soil moisture and streamflow.
# 1. **Y2018M09D05_RH_DS_Cat_Label_V01**  
#     Add category and Label for drought severity.
# 
# 
# 
# 
# 
# ### Riverine and Coastal Flood Risk | RFR CFR
# 
# 1.  **Y2018M12D04_RH_RFR_CFR_BQ_V01**
#     Process flood risk data and store on BigQuery.
#     
# 
# ### Country Shapefile Branch
# 
# ### Other Indicators Branch
# 
# 
# #### Drought Risk
# 1. **Y2018M09D28_RH_DR_Ingest_EE_V01**  
#     Ingest drought risk data in earthengine. 
# 1. **Y2018M09D28_RH_DR_Zonal_Stats_EE_V01**  
#     Drought Risk zonal stats in earthengine.
# 1. **Y2018M09D28_RH_DR_Cat_Label_V01**  
#     Merge, cleanup, add category and label for drought risk.
#     
# #### ICEP
# 1. **Y2018M10D01_RH_ICEP_Basins_PostGIS_V01**  
#     Store ICEP data in PostGIS Database.
# 1. **Y2018M11D22_RH_ICEP_Basins_To_EE_V01**  
#     Rasterize and store shape and raste ICEP data in earthengine and S3. 
# 1. **Y2018M11D22_RH_ICEP_Zonal_Stats_Hybas6_EE_V01**  
#     Zonal statistics icep_raw at hydrobasin level 6.
# 1. **Y2018M11D22_RH_ICEP_Hybas6_Cat_Label_BQ_V02**  
#     Cleanup, add category and label for icep at hydrobasin level 6.
#     
#     
# #### Untreated Collected Wastewater | UCW
# 1.  **Y2018M12D04_RH_UCW_BQ_V01**  
#     Process wastewater data and store on BigQuery.
# 
# #### Unimproved/no Drinking Water | UDW
# 1. **Y2018M12D05_RH_UDW_BQ_V01**
#     Process unimproved/no drinking water and store on BigQuery.
#     
# #### Unimproved/no Sanitation | USA
# 1. **Y2018M12D05_RH_USA_BQ_V01**  
#     Process unimproved/no sanitation and store on BigQuery.
# 
# #### RepRisk Index | RRI
# 1. **Y2018M12D05_RH_RRI_BQ_V01**  
#     Process reprisk index and store on BigQuery.
# 
# 
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
# 
# 1. **Y2017M11D16_RH_Add_Water_Stress_postGIS_V01 #NOT USED**    
#     add pcrGlob water demand, supply and stress data to database.
#     
# 
# 1. **Y2018M12D06_RH_Process_Weights_BQ_V01**  
#     Process and upload industry weights to BigQuery.
#     
# 1. **Y2018M12D07_RH_Process_Area_BQ_V01**  
#      Process and upload areas of master shapefile to BigQuery.
#     
# 
# ### Geospatial Layers
# 
# some geospatial layers are already on RDS as a result of a previous step. These layers include: Hydrobasins, ICEP
# 
# 1.  **Y2018M11D14_RH_Create_Geospatial_Dataset_BQ_V01**  
#     Create dataset to hold geospatial tables in Bigquery.
# 
# 1.  **Y2018M11D12_RH_Hybas_RDS_to_BQ_V01**  
#     Upload hydrobasin geospatial data to bigquery.  
# 1.  **Y2018M11D12_RH_GADM36_Level1_to_RDS_V01**  
#     Upload GADM 3.6 level 1 to RDS.    
# 1. **Y2018D12D17_RH_GADM36L01_EE_V01**  
#     Ingest GADM level 1 data to earthengine. 
# 1. **Y2019M01D07_RH_GADM36L01_Rasterize_EE_V01**  
#     Rasterize GADM level 1 using earthengine.
# 1.  **Y2018M11D12_RH_GADM36_Level1_RDS_to_BQ_V01**  
#     Upload GADM 3.6 level 1 to bigquery.
# 1. **Y2018M11D14_RH_WHYMAP_to_RDS_V01**  
#     Upload WHYMAP geospatial data to RDS.  
# 1. **Y2018M11D14_RH_WHYMAP_RDS_to_BQ_V01**  
#     Upload WHYMAP to BQ.    
# 1. **Y2018M11D14_RH_ICEPBasins_To_BQ_V01**  
#     Upload ICEP Basins to BQ.
# 1. **Y2018M12D04_RH_Union_ArcMap_V01**  
#     Union of hybas6, gadm36L01, Whymap in Arcmap
# 1. **Y2018M12D06_RH_Master_Shape_Composite_Index_V01**  
#     Simplify and create composite index.
# 1. **Y2018M12D06_RH_Master_Shape_Dissolve_V01**  
#     Dissolve on composite index in arcMap.
# 1. **Y2018M12D06_RH_Master_Shape_V01**  
#     Process master shapefile and store in multiple formats.
#     
#     
# 
# 
#     
#     
# 
# ### Merge Rawdata
# 
#     
# 1. **Y2018M12D04_RH_Master_Merge_Rawdata_GPD_V02**  
#     Merge raw data into master table. 
# 1. **Y2018M12D11_RH_Master_Weights_GPD_V02**  
#     Apply industry weights on merged table.
# 1. **Y2018M12D14_RH_Master_Horizontal_GPD_V01**  
#     Create horizontal table for readability. 
#     
#     
# 
# ### Share Data
# 1.  **Y2019M01D14_RH_Aqueduct_Results_V01**  
#     Share Aqueduct results with external party in multiple formats. 
#     
#     
# ## Results at GADM Level
# Scripts used for aggregations have GA in them to denote GADM Aggregations. 
# 
# 
# 1.  **Y2019M01D08_RH_Total_Demand_EE_V01**  
#     Total demand to be used as weights for spatial aggregation.
# 1.  **Y2019M01D10_RH_GA_Rasterize_Indicators_GDAL_V01**  
#     Rasterize indicators at 30s as input for aggregation.
# 1.  **Y2019M01D10_RH_GA_Rasterize_Indicators_EE_V01**  
#     Ingest rasterized indicators in earthengine. 
# 1.  **Y2019M01D17_RH_GA_Zonal_Stats_Weighted_Indicators_EE_V01**  
#     Zonal statistics for GADM level 1 for sum  of weights and weights * indicators.
# 
# 1.  **Y2019M01D29_RH_GA_GTD_Cat_Label_V01**  
#     Add category and label for groundwater stress and trend at state level.
# 
# 1.  **Y2019M01D07_RH_GA_CEP_Zonal_Stats_GADM_EE_V01**  
#     Zonal statistics icep_raw at GADM level 1.
# 1.  **Y2019M01D07_RH_GA_CEP_GADM_Cat_Label_BQ_V01**  
#     Cleanup, add category and label for icep at gadm level 1.
# 1.  **Y2019M01D29_RH_GA_DR_Zonal_Stats_GADM_EE_V01**  
#     Zonal statistics drought risk at GADM level 1.
#     
#     
# 1.  **Y2019M01D28_RH_GA_Zonal_Stats_Table_V01**  
#     Post process aggregations from EE and combine with other datasets.
# 
# 
# 
# 
#     
#     
#     
# ### Failed Union Approaches
# 1. **Y2018M11D14_RH_Hybas_Union_GADM_BQ_V02 (Not Used)**  
#     Union of Hybas and GADM in Bigquery.  
# 1. **Y2018M11D14_RH_hybasgadm_Union_whymap_BQ_V01 (Not Used)**  
#     Union of hybasgadm and Whymap in Bigquery.
# 1. **Y2018M11D21_RH_Hybasgadmwhymap_Union_Icep_BQ_V01 (Not Used)**  
#     Union of hybasgadmwhymap and Icepbasins in Biqguery.    
# 1. **Y2018M11D28_RH_hybasgadmwhymap_To_GCS_V01 (Not Used)**  
#     Export table with geographies as WKT to CSV file on GCS.     
# 1. **Y2018M11D26_RH_Hybasgadmwhymap_Union_To_Gpkg_V01 (Not Used)**  
#     Convert union of hybas, gadm and whymap to geopackage.    
# 1. **Y2018M11D28_RH_Hybasgadmwhymap_GCS_To_Gpkg_V01 (Not Used)**  
#     Convert hybasgadmwhymap csv files from GCS to geopackage.  
# 1. **Y2018M11D28_RH_Hybas_Union_GADM36L01_GPD_V01 (Not Used)**  
#     Union of hydrobasin and GADM 36 level 1 using geopandas.    
# 1.  **Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_V01 (Not Used)**  
#     Union of hydrobasin and GADM 36 level 1 using geopandas parallel processing.
# 1.  **Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_Merge_V01 (Not Used)**  
#     Union of hydrobasin and GADM 36 level 1 merge results. 
# 1.  **Y2018M12D03_RH_Hybas6GADM36L01_U_Whymap_PP_V01 (Not Used)**  
#     Union of hydrobasingadm36L01 and Whymap using geopandas parallel processing.
# 1. **Y2018M11D29_RH_Hybas6_U_GADM36L01_PostGIS_V01 (Not Used)**  
#     Union of hydrobasin and GADM 36 level 1 using postGIS
#     
# 
# ### Evaporative Stress Index  
# 1.  **Y2018M03D22_RH_Download_ESI_V01**  
#     Download ESI (Evaporative Stress Index) to Instance, Google Cloud Services and Amazon S3.
# 
#     
# 
# 

# In[ ]:



