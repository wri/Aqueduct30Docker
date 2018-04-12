
# coding: utf-8

# In[1]:

""" Convert Indicators from ASCII to Geotiff
-------------------------------------------------------------------------------
A couple of indicators are shared in ASCII format. Converting to geotiff and
upload to GCS and AWS.


Author: Rutger Hofste
Date: 20180327
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name


Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V02"

S3_INPUT_PATH 



EC2_INPUT_PATH = "/volumes/data/Y2017M07D31_RH_download_PCRGlobWB_data_V01/output"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/output"
EC2_INPUT_PATH_ADDITIONAL = "/volumes/data/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/input"
S3_INPUT_PATH_ADDITIONAL = "s3://wri-projects/Aqueduct30/rawData/WRI/samplegeotiff/"
GCS_OUTPUT = "gs://aqueduct30_v01/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/output/"
EE_OUTPUT_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V05/"

# Output Parameters


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')
get_ipython().system('mkdir -p {EC2_INPUT_PATH_ADDITIONAL}')
get_ipython().system('aws s3 cp {S3_INPUT_PATH_ADDITIONAL} {EC2_INPUT_PATH_ADDITIONAL} --recursive')


# In[3]:

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
    
from netCDF4 import Dataset
import os
import datetime
import subprocess
import pandas as pd
import re
import time
from datetime import timedelta


# ## Functions

# In[4]:

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

def splitKey(key):
    # will yield the root file code and extension of a set of keys
    prefix, extension = key.split(".")
    fileName = prefix.split("/")[-1]
    values = re.split("_|-", fileName)
    keyz = ["geographic_range","indicator","spatial_resolution","temporal_range_min","temporal_range_max"]
    outDict = dict(zip(keyz, values))
    outDict["fileName"]=fileName
    outDict["extension"]=extension
    return outDict


def uploadEE(index,row):
    target = EE_OUTPUT_PATH + row.fileName
    source = GCS_OUTPUT + row.fileName + "." + row.extension
    metadata = "--nodata_value=%s -p extension=%s -p filename=%s -p geographic_range=%s -p indicator=%s -p spatial_resolution=%s -p temporal_range_max=%s -p temporal_range_min=%s -p units=%s -p ingested_by=%s -p exportdescription=%s" %(row.nodata,row.extension,row.fileName,row.geographic_range,row.indicator,row.spatial_resolution,row.temporal_range_max,row.temporal_range_min, row.units, row.ingested_by, row.exportdescription)
    command = "/opt/anaconda3/bin/earthengine upload image --asset_id %s %s %s" % (target, source,metadata)
    try:
        response = subprocess.check_output(command, shell=True)
        outDict = {"command":command,"response":response,"error":0}
        df_errors2 = pd.DataFrame(outDict,index=[index])
        pass
    except:
        try:
            outDict = {"command":command,"response":response,"error":1}
        except:
            outDict = {"command":command,"response":-9999,"error":2}
        df_errors2 = pd.DataFrame(outDict,index=[index])
        print("error")
    return df_errors2

    


# In[5]:

inputLocationSampleGeotiff = os.path.join(EC2_INPUT_PATH_ADDITIONAL,"sampleGeotiff.tiff")


# In[6]:

print(inputLocationSampleGeotiff)


# In[7]:

[xsizeSample,ysizeSample,geotransformSample,geoprojSample,ZSample] = readFile(inputLocationSampleGeotiff)


# In[8]:

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

# In[9]:

get_ipython().system('gsutil -m cp {EC2_OUTPUT_PATH}/*.tif {GCS_OUTPUT}')


# The next step is to ingest these rasters to earthengine with appropriate metadata

# In[10]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls %s") %(GCS_OUTPUT)


# In[11]:

print(command)


# In[12]:

keys = subprocess.check_output(command,shell=True)


# In[13]:

keys = keys.decode('UTF-8').splitlines()


# In[14]:

print(keys)


# In[15]:

df = pd.DataFrame()
i = 0
for key in keys:
    i = i+1
    outDict = splitKey(key)
    df2 = pd.DataFrame(outDict,index=[i])
    df = df.append(df2)   


# In[16]:

df.head()


# In[17]:

df["nodata"] = -9999
df["ingested_by"] ="RutgerHofste"
df["exportdescription"] = df["indicator"]
df["units"] = "dimensionless"


# In[18]:

df


# In[19]:

df_errors = pd.DataFrame()
start_time = time.time()
for index, row in df.iterrows():
    elapsed_time = time.time() - start_time 
    print(index,"%.2f" %((index/9289.0)*100), "elapsed: ", str(timedelta(seconds=elapsed_time)))
    df_errors2 = uploadEE(index,row)
    df_errors = df_errors.append(df_errors2)


# For the Threshold setting, copying these rasters to S3. 

# In[20]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[21]:

df_errors


# In[ ]:



