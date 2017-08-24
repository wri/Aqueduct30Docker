
# coding: utf-8

# ### Buffer FAO Names hydrobasins
# 
# * Purpose of script: Buffer FAO Names hydrobasins to avoid assigning names to basins that only slightly touch other polygons
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170823

# https://gist.github.com/tmcw/3987659

# In[1]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Merge_FAONames_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Buffer_FAONames_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D23_RH_Buffer_FAONames_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D23_RH_Buffer_FAONames_V01/output/"
INPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_v01.shp"
OUTPUT_FILE_NAME = "hydrobasins_fao_fiona_merged_buffered_v01.shp"
OUTPUT_FILE_NAME_PROJ = "hydrobasins_fao_fiona_merged_buffered_v01_backup.prj"
BUFFERDIST = -0.005 # Buffer distance in Degrees 


# In[2]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[3]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --quiet')


# In[4]:

import os
if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'
from osgeo import gdal,ogr,osr
'GDAL_DATA' in os.environ
# If false, the GDAL_DATA variable is set incorrectly. You need this variable to obtain the spatial reference


# In[5]:

inputLocation = os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME)
outputLocation = os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME)


# In[6]:

os.environ['SHAPE_ENCODING'] = "utf-8"


# In[7]:

def createBuffer(inputfn, outputBufferfn, bufferDist):
    inputds = ogr.Open(inputfn)
    inputlyr = inputds.GetLayer()
    featureCount = inputlyr.GetFeatureCount()
    print(featureCount)

    shpdriver = ogr.GetDriverByName('ESRI Shapefile')
    if os.path.exists(outputBufferfn):
        shpdriver.DeleteDataSource(outputBufferfn)
    outputBufferds = shpdriver.CreateDataSource(outputBufferfn)
    dest_srs = ogr.osr.SpatialReference()
    dest_srs.ImportFromEPSG(4326)
    bufferlyr = outputBufferds.CreateLayer(outputBufferfn,dest_srs, geom_type=ogr.wkbPolygon)
    featureDefn = bufferlyr.GetLayerDefn()
    featureDefn2 = inputlyr.GetLayerDefn()
    
    i = 0
    for feature in inputlyr:
        if i % 500 ==0:
            print("Percentage Completed: %0.2d"% ((i/featureCount)*100))
        i += 1
            
        ingeom = feature.GetGeometryRef()
        geomBuffer = ingeom.Buffer(bufferDist)

        outFeature = ogr.Feature(featureDefn)
        outFeature.SetGeometry(geomBuffer)
        bufferlyr.CreateFeature(outFeature)
        outFeature = None


# In[8]:

createBuffer(inputLocation, outputLocation, BUFFERDIST)


# In[9]:

outputLocationProj = os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME_PROJ)


# The next command will create a secondary .prj file in case the crs of the original file is missing

# In[10]:

spatialRef = osr.SpatialReference()
spatialRef.ImportFromEPSG(4326)
print(spatialRef)
spatialRef.MorphToESRI()
file = open(outputLocationProj, 'w')
file.write(spatialRef.ExportToWkt())
file.close()


# In[11]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive --quiet')

