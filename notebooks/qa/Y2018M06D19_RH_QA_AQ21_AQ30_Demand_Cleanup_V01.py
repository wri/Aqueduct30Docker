
# coding: utf-8

# In[ ]:

""" Combine zonal statistics of different indicators and calculate flux. 
-------------------------------------------------------------------------------
zonal_stats_ca_aq21ee_export.csv

Author: Rutger Hofste
Date: 20180619
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:


"""

OVERWRITE = 1
TESTING = 0
SCRIPT_NAME = "Y2018M06D19_RH_QA_AQ21_AQ30_Demand_Cleanup_V01"
OUTPUT_VERSION = 1

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M06D18_RH_QA_AQ21_AQ30_Demand_Zonal_Stats_EE_V01/output_V02"

AQ21_SHAPEFILE_S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/qaData/Y2018M06D05_RH_QA_Aqueduct21_Flux_Shapefile_V01/output_V05"
AQ30_SHAPEFILE_S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V02/output_V04/"

AQ21_INPUT_FILE_NAME = "aqueduct21_flux"
AQ30_INPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_V04"

ECKERT_IV_PROJ4_STRING = "+proj=eck4 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"


ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/qaData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input GCS : " + GCS_INPUT_PATH +
      "\nInput ec2: " + ec2_input_path + 
      "\nOutput ec2: " + ec2_output_path +
      "\nOutput s3: " + ec2_output_path)


# In[ ]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[ ]:

import geopandas as gpd
import pandas as pd


# In[ ]:

if OVERWRITE:
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')
else: 
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')


# In[ ]:

# Aq 21 shapefile
get_ipython().system('aws s3 cp {AQ21_SHAPEFILE_S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[ ]:

# Aq 30 shapefile
get_ipython().system('aws s3 cp {AQ30_SHAPEFILE_S3_INPUT_PATH} {ec2_input_path} --recursive --exclude "*" --include "hybas_lev06_v1c_merged_fiona_V04*"')


# In[ ]:

# Zonal Stats

get_ipython().system('gsutil cp {GCS_INPUT_PATH}/* {ec2_input_path}')


# In[ ]:

# Read Shapefiles of Aqueduct 2.1 and 3.0

aq21_input_file_path = "{}/{}.shp".format(ec2_input_path,AQ21_INPUT_FILE_NAME)
gdf_aq21 = gpd.read_file(aq21_input_file_path )
gdf_aq21 = gdf_aq21.set_index("GU")

aq30_input_file_path = "{}/{}.shp".format(ec2_input_path,AQ30_INPUT_FILE_NAME)
gdf_aq30 = gpd.read_file(aq30_input_file_path )

gdf_aq30_eckert4 = gdf_aq30.to_crs(ECKERT_IV_PROJ4_STRING)
gdf_aq30["area_m2"] = gdf_aq30_eckert4.geometry.area
gdf_aq30 = gdf_aq30.set_index("PFAF_ID")



# In[ ]:

aqueduct_versions = ["aq21","aq30"]
sectors = ["a","d","i","t"]
demand_types = ["c","u"]

for aqueduct_version in aqueduct_versions:  
    
    if aqueduct_version == "aq21":
        gdf_left = gdf_aq21.copy()
        index_name = "GU"
    elif aqueduct_version == "aq30":
        gdf_left = gdf_aq30.copy()
        index_name = "PFAF_ID"
    else:
        break
    
    for demand_type in demand_types:
        for sector in sectors:
            print(aqueduct_version,demand_type,sector)
            input_file_name = "zonal_stats_{}{}_{}ee_export.csv".format(demand_type,sector,aqueduct_version)
            input_file_path = ec2_input_path + "/" + input_file_name
            df_in = pd.read_csv(input_file_path)
            
            df_out = df_in[["sum","count",index_name]].copy()
            df_out = df_out.set_index(index_name)
            
            df_out = df_out.rename(columns={"sum":"sum_{}{}_m3".format(demand_type,sector),
                                            "count":"count_{}{}_dimensionless".format(demand_type,sector)})
            gdf_left  = gdf_left.merge(right=df_out,
                                   how="left",
                                   left_index = True,
                                   right_index = True)
    gdf_out = gdf_left
    df_out = pd.DataFrame(gdf_out.drop("geometry",1))
    output_file_path_no_ext = "{}{}".format(ec2_output_path,aqueduct_version)
    
    gdf_left.to_file(driver='ESRI Shapefile', filename=output_file_path_no_ext+".shp")
    df_out.to_csv(output_file_path_no_ext+".csv")


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 
