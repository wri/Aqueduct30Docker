
# coding: utf-8

# In[1]:

""" Create a negative buffer for the FAO basins to avoid sliver polygons.
-------------------------------------------------------------------------------
Buffer FAO Names hydrobasins to avoid assigning names to basins that only 
slightly touch other polygons

Author: Rutger Hofste
Date: 20170823
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.    

"""

SCRIPT_NAME = "Y2017M08D23_RH_Buffer_FAONames_V01"
OUTPUT_VERSION = 2

BUFFERDIST = -0.002 # Buffer distance in Degrees, set so that 15 arc s will not cause any problems with a negative number
RESOLUTION = 3 # number of point per quarter arc
TESTING = 0
INDEX = 0

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Merge_FAONames_V01/output_V02/"

INPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_v01.shp"
OUTPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_buffered_v01.shp"
OUTPUT_FILE_NAME_PROJ = "hydrobasins_fao_fiona_merged_buffered_v01_backup.prj"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_input_path = "s3://wri-projects/Aqueduct30/processData/{}/input_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ec2: " + ec2_input_path,
      "\nInput s3: " + S3_INPUT_PATH ,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --quiet')


# In[5]:

import os
if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'
from osgeo import gdal,ogr,osr
'GDAL_DATA' in os.environ
# If false, the GDAL_DATA variable is set incorrectly. You need this variable to obtain the spatial reference
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import time
get_ipython().magic('matplotlib notebook')

from shapely.wkt import loads
from shapely.geometry import Point


# In[6]:

INPUTSHP = os.path.join(ec2_input_path,INPUT_FILE_NAME)
OUTPUTSHP = os.path.join(ec2_output_path,OUTPUT_FILE_NAME)

# This function can be optimized in the future by using: https://gis.stackexchange.com/questions/253224/geopandas-buffer-using-geodataframe-while-maintaining-the-dataframe
def buffer(INPUTSHP,BUFFERDIST,RESOLUTION,OUTPUTSHP):
    #INPUTSHP path to shapefile
    #INDEX name of index column, can be set to None if you want geopandas to create a new index column. Index must be unique
    #BUFFERDIST buffer distance in degrees,can also be negative
    #RESOLUTION number of points per quarter circle. See shapely / geopandas docs for documentation
    #OUTPUTSHP path to output shapefile
    print("1/3 Reading file: ", INPUTSHP)
    gdf =  gpd.read_file(INPUTSHP)
    try:
        gdf = gdf.set_index(INDEX)
        gdf['index_copy'] = gdf.index
        
    except:
        gdf['index1'] = gdf.index
        gdf['index_copy'] = gdf['index1']
        
    dfIn = gdf
    dfIn = dfIn.drop('geometry',1)
    print("2/3 Creating buffer")
    gsBuffer = gdf['geometry'].buffer(BUFFERDIST,resolution=RESOLUTION)
    gdfBuffer =gpd.GeoDataFrame(geometry=gsBuffer)
    gdfBuffer['index_copy'] = gdfBuffer.index
    gsArea = gdfBuffer.geometry.area
    dfArea = pd.DataFrame(gsArea)
    dfArea.columns = ['area']
    dfArea['index_copy'] = dfArea.index
    dfValidArea = dfArea.loc[dfArea['area'] > 0]
    dfInValidArea = dfArea.loc[dfArea['area'] <= 0]
    gdfTemp = gdfBuffer.merge(dfValidArea,how="inner",on="index_copy")
    gdfOut = gdfTemp.merge(dfIn,how="left",on="index_copy")
    gdfOut = gdfOut.set_index("index_copy")
    print("3/3 Writing output")
    gdfOut.to_file(OUTPUTSHP)
    print("file saved to: ",OUTPUTSHP)    
    return dfInValidArea    


# In[7]:

dfInValidArea = buffer(INPUTSHP,BUFFERDIST,RESOLUTION,OUTPUTSHP)


# In[8]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive --quiet')


# The following polygons were removed from the dataset

# In[9]:

dfInValidArea


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:02:49.129215
