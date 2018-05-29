
# coding: utf-8

# 
# 
# 
# 
# 
#     
# ### PCRGlobWB 2.0
# 
# 1. **Y2018M05D08_RH_Create_Zones_Mask_30sPfaf06_EE_V01** (Archive)  
#     Use the masked max riverdischarge flow accumulation and zones to create final masked zones image.
# 1. **Y2018M05D03_RH_Max_FA_Add_Sinks_EE_V01** (Archive)  
#     Use flow accumulation and sinks to create second mask for riverdischarge.
# 1. **Y2018M05D09_RH_Mask_Sinks_EE_V01**(Not used)  
#     Mask sinks that have already been used in the max flow accumumation scripts.  
#     mask sinks if streamorder > max streamorder for main river. 5min
# 1. **Y2018M05D04_RH_Zonal_Stats_Supply_EE_V01**  
#     Zonal statistics for basin supply. Export in table format.
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
#     February 27th 2018, based on a discusssion at the Utrecht University I will have to take a slightly different approach. 
#     This includes the grouping of delta basins and calculating a running average. I will put the unused scripts in an archive     folder. I also cleaned up the scripts and assets in GEE 
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
# 1. **Y2017M12D28_RH_EE_Results_To_PostGIS_V01**  
#     Store earth engine raster results in postgis. 
# 1.  **Y2017M08D02_RH_Upload_to_GoogleCS_V02**(obsolete as of 20180416)  
#     Upload files to Google Cloud Storage.     
# 1. **Y2017M08D30_RH_Average_Supply_EE_V01**  
#     This script will canculate the average PCRGlobWB2.0 supply (local runoff) using the ee python API.
# 1. **Y2017M09D01_RH_linear_trend_Ag_Demand_EE_V01**  
#     Due to the sensitivity of the model to irrigation demand we take the linear trend of 2004 - 2014 for ag demand.
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
