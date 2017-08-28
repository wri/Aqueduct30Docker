
# coding: utf-8

# ### Buffer FAO Names hydrobasins
# 
# * Purpose of script: Buffer FAO Names hydrobasins to avoid assigning names to basins that only slightly touch other polygons
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170823

# In[1]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Merge_FAONames_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Buffer_FAONames_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D23_RH_Buffer_FAONames_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D23_RH_Buffer_FAONames_V01/output/"
INPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_v01.shp"
OUTPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_buffered_v01.shp"
OUTPUT_FILE_NAME_PROJ = "hydrobasins_fao_fiona_merged_buffered_v01_backup.prj"
BUFFERDIST = -0.002 # Buffer distance in Degrees, set so that 15 arc s will not cause any problems with a negative number
RESOLUTION = 3 # number of point per quarter arc
TESTING = False
INDEX = None


# In[2]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --quiet')


# In[3]:

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


# In[7]:

INPUTSHP = os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME)
OUTPUTSHP = os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME)

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


# In[8]:

dfInValidArea = buffer(INPUTSHP,BUFFERDIST,RESOLUTION,OUTPUTSHP)


# In[9]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive --quiet')


# The following polygons were removed from the dataset

# In[11]:

dfInValidArea


# In[ ]:



