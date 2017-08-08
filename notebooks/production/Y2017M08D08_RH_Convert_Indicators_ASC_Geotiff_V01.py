
# coding: utf-8

# # Convert Indicators from ASCII to Geotiff
# 
# * Purpose of script: Some Utrecht Indicators are shared in Ascii format. This script converts them to geotiff and uploads to GCS
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170808

# ## Settings

# In[26]:

EC2_INPUT_PATH = "/volumes/data/Y2017M07D31_RH_download_PCRGlobWB_data_V01/output"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/output"
EC2_INPUT_PATH_ADDITIONAL = "/volumes/data/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/input"
S3_INPUT_PATH_ADDITIONAL = "s3://wri-projects/Aqueduct30/rawData/WRI/samplegeotiff/"
GCS_OUTPUT = "gs://aqueduct30_v01/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/output/"


# In[16]:

get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')
get_ipython().system('mkdir -p {EC2_INPUT_PATH_ADDITIONAL}')
get_ipython().system('aws s3 cp {S3_INPUT_PATH_ADDITIONAL} {EC2_INPUT_PATH_ADDITIONAL} --recursive')


# In[17]:

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
    
from netCDF4 import Dataset
import os
import datetime
import subprocess


# ## Functions

# In[18]:

def readFile(filename):
    filehandle = gdal.Open(filename)
    band1 = filehandle.GetRasterBand(1)
    geotransform = filehandle.GetGeoTransform()
    geoproj = filehandle.GetProjection()
    Z = band1.ReadAsArray()
    xsize = filehandle.RasterXSize
    ysize = filehandle.RasterYSize
    filehandle = None
    return xsize,ysize,geotransform,geoproj,Z

def writeFile(filename,geotransform,geoprojection,data):
    (x,y) = data.shape
    format = "GTiff"
    driver = gdal.GetDriverByName(format)
    # you can change the dataformat but be sure to be able to store negative values including -9999
    dst_datatype = gdal.GDT_Float32
    dst_ds = driver.Create(filename,y,x,1,dst_datatype, [ 'COMPRESS=LZW' ])
    dst_ds.GetRasterBand(1).SetNoDataValue(-9999)
    dst_ds.GetRasterBand(1).WriteArray(data)
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(geoprojection)
    dst_ds = None
    return 1


# In[19]:

inputLocationSampleGeotiff = os.path.join(EC2_INPUT_PATH_ADDITIONAL,"sampleGeotiff.tiff")


# In[20]:

print(inputLocationSampleGeotiff)


# In[21]:

[xsizeSample,ysizeSample,geotransformSample,geoprojSample,ZSample] = readFile(inputLocationSampleGeotiff)


# In[23]:

files = os.listdir(EC2_INPUT_PATH)
newExtension =".tif"
for oneFile in files:
    if oneFile.endswith(".asc"):
        base , extension = oneFile.split(".")
        xsize,ysize,geotransform,geoproj,Z = readFile(os.path.join(EC2_INPUT_PATH,oneFile))
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        outputFileName = base + newExtension
        writeFile(os.path.join(EC2_OUTPUT_PATH,outputFileName),geotransformSample,geoprojSample,Z)


# Upload to GCS

# In[25]:

get_ipython().system('gsutil -m cp {EC2_OUTPUT_PATH}/*.tif {GCS_OUTPUT}')


# For the Threshold setting, copying these rasters to S3. 

# In[27]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:



