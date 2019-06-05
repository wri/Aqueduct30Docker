
# coding: utf-8

# # Comparing Aqueduct 3.0 results with Aqueduct 2.1 
# 
# 
# ## Approach
# This is a limited comparison of selected results of Aqueduct 3.0 and Aqueduct 2.1. In the future, a more comprehensive comparison might be desired. The objective is to display some of the major differences between the new and the old versions without explaining the reasons of those differences. 
# 
# 
# ### Overall water risk
# 
# Comparison of distribution based on vector data
# 
# A rasterized approach (% difference or different categories). 
# 
# 
# ### Baseline water stress, interannual variability, seasonal variability
# 
# Comparison of distribution based on vector data
# 
# A rasterized approach (% difference or different categories). 
# 
# 
# 
# ## Scripts
# 
# 1. **Y2019M05D21_RH_AQ30VS21_Rasterize_AQ30_EE_V01**  
#     Rasterize Aqueduct 30 and store to Google Cloud Storage.
# 1. **Y2019M05D22_RH_AQ30VS21_Rasters_AQ30_Ingest_EE_V01**  
#     Ingest rasterized AQ30 indicators to earthengine.
# 1. **Y2019M05D22_RH_AQ39VS21_Rasterize_AQ21_EE_V01**  
#     Rasterize Aqueduct 21 and store to Google Cloud Storage.
# 1. **Y2019M05D22_RH_AQ30VS21_Rasters_AQ21_Ingest_EE_V01**  
#     Ingest rasterized AQ21 indicators to earthengine.   
# 1. **Y2019M05D22_RH_AQ30VS21_Compare_Tables_V01**  
#     Compare Aqueduct 30 vs 21 and create change tables.
# 1. **Y2019M05D23_RH_AQ30VS21_Combine_Tables_V01**  
#     Create tables with aqueduct 30 categories vs aqueduct 21. 
# 1. **Y2019M05D28_RH_AQ30VS21_Export_Dif_Geotiff_EE_V01**  
#     Generate and export difference maps of various indicators.
# 1. **Y2019M05D30_RH_AQ30VS21_Merge_Dif_Geotiff_V01**  
#     Merge the difference geotiffs into one.
#     
#    
# 
# 
# 

# In[ ]:



