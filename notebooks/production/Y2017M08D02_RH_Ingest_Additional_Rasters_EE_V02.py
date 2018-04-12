
# coding: utf-8

# # Ingest Additional Rasters on Earth Engine
# 
# * Purpose of script: This notebook will ingest some of the missing rasters to Earth Engine like streamflow direction, DEM etc. 
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170802

# ## Preparation
# 
# 1. gcloud authorization (`gcloud init`)
# 1. earthengine authorization (`earthengine authorize`)
# 1. aws authorization (`aws configure`)
# 

# In[1]:

S3_INPUT_PATH_SAMPLE = "s3://wri-projects/Aqueduct30/rawData/WRI/samplegeotiff"
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/Utrecht/additionalFiles/flowNetwork/topo_pcrglobwb_05min"
INPUTLOCATION_SAMPLE_GEOTIFF = "/volumes/data/PCRGlobWB20V01/additional/sampleGeotiff.tiff"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/output"
GCS_PATH = "gs://aqueduct30_v01/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/output/"
EE_BASE = "projects/WRI-Aquaduct/PCRGlobWB20V07/"


# In[2]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[3]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_SAMPLE} {EC2_INPUT_PATH} --recursive')


# Check if the file is actually copied

# ## Script

# Create working environment and copy relevant files. 

# In[4]:

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
    
from netCDF4 import Dataset
import os
import time
from datetime import timedelta
import datetime
import subprocess
import pandas as pd
import re


# In[5]:

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
    prefix, extension = key.split(".")
    fileName = prefix.split("/")[-1]
    values = re.split("_|-", fileName)
    keyz = ["indicator","spatial_resolution","units"]
    outDict = dict(zip(keyz, values))
    outDict["fileName"]=fileName
    outDict["extension"]=extension
    return outDict

def uploadEE(index,row):
    target = EE_BASE + row.fileName
    source = GCS_PATH + row.fileName + "." + row.extension
    metadata = "--nodata_value=%s -p extension=%s -p filename=%s -p indicator=%s -p spatial_resolution=%s -p units=%s -p ingested_by=%s -p exportdescription=%s" %(row.nodata,row.extension,row.fileName,row.indicator,row.spatial_resolution, row.units, row.ingested_by, row.exportdescription)
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

   
    


# extension                                                           tif
# fileName              global_droughtseveritystandardisedsoilmoisture...
# geographic_range                                                 global
# indicator                       droughtseveritystandardisedsoilmoisture
# spatial_resolution                                                 5min
# temporal_range_max                                                 2014
# temporal_range_min                                                 1960
# nodata                                                            -9999
# ingested_by                                                RutgerHofste
# exportdescription               droughtseveritystandardisedsoilmoisture

# In[6]:

[xsizeSample,ysizeSample,geotransformSample,geoprojSample,ZSample] = readFile(os.path.join(EC2_INPUT_PATH,"sampleGeotiff.tiff"))


# ## PCRGlobWB auxiliary datasets
# 
# after receiving the results from Yoshi, there was a fair amount of information missing such as flow direction, basins etc. Rens van Beek has been very supportive and provided WRI with some auxiliary datasets. This script will put them in the right format, upload to GCS and ingest them to Earth Engine.  
# 

# In[7]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive --quiet')


# Copy to EC2 instance

# renaming the files with a structured name indicator_5min_unit.map
# 
# copy from readme.txt file on S3
# 
# 
# Data received from Rens van Beek on Feb 24 2017. Rutger Hofste
# topo_pcrglobwb_05min.zip  
# 
# |Archive Length |   Date |   Time |    Name | Units | newName | 
# |---:|---:|---:|---:| ---:| ---:|
# |37325056|  02-24-2017| 15:46 |  accumulated_drainage_area_05min_sqkm.map| $$km^2$$ |accumulateddrainagearea_05min_km2.map |
# |37325056|  02-24-2017 |15:45 |  cellsize05min.correct.map| $$m^2$$ |cellsize_05min_m2.map |
# |37325056| 02-24-2017| 15:44 |  gtopo05min.map| $$m$$ |gtopo_05min_km2.map|
# |9331456|  02-24-2017| 15:45 |   lddsound_05min.map| numpad |lddsound_05min_numpad.map |
# |121306624| | |               4 files| | |
# 
# All files are 5 arc minute maps in PCRaster format and WGS84 projection (implicit).
# The gtopo05min.map is the DEM from the gtopo30 dataset that we use for downscaling meteo data occasionally. This is consistent with the CRU climate data sets and the hydro1k drainage dataset. Elevation is in **metres**. The cellsize05.correct.map is the surface area of a geographic cell in **m2** per 5 arc minute cell. The lddsound_05min.map is identical to the LDD we sent you earlier with the **8-point pour algorithm**.(numpad e.g. 7 is NW 6 is E etc.) The accumulated_drainage_area_05min_sqkm.map is the accumulated drainage area in **km2** per cell along the LDD.
# 
# 
# 
# 

# In[8]:

get_ipython().system('mv {EC2_INPUT_PATH}/accumulated_drainage_area_05min_sqkm.map {EC2_INPUT_PATH}/accumulateddrainagearea_05min_km2.map')
get_ipython().system('mv {EC2_INPUT_PATH}/cellsize05min.correct.map {EC2_INPUT_PATH}/cellsize_05min_m2.map')
get_ipython().system('mv {EC2_INPUT_PATH}/gtopo05min.map {EC2_INPUT_PATH}/gtopo_05min_m.map')
get_ipython().system('mv {EC2_INPUT_PATH}/lddsound_05min.map {EC2_INPUT_PATH}/lddsound_05min_numpad.map')



# In[9]:

files = os.listdir(EC2_INPUT_PATH)


# In[10]:

print(files)


# In[11]:

newExtension =".tif"
for oneFile in files:
    if oneFile.endswith(".map"):
        print(oneFile)
        base , extension = oneFile.split(".")
        xsize,ysize,geotransform,geoproj,Z = readFile(os.path.join(EC2_INPUT_PATH,oneFile))
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        outputFileName = base + newExtension
        writeFile(os.path.join(EC2_OUTPUT_PATH,outputFileName),geotransformSample,geoprojSample,Z)
        


# Upload Auxiliary files to GCS

# In[12]:

get_ipython().system('gsutil -m cp {EC2_OUTPUT_PATH}/*.tif {GCS_PATH}')


# Ingest GCS data in earthengine. Some metadata is missing, therefore 

# In[12]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls %s") %(GCS_PATH)


# In[13]:

keys = subprocess.check_output(command,shell=True)
keys = keys.decode('UTF-8').splitlines()


# In[14]:

print(keys)


# Upload Rasterized HydroBasin files to GCS

# In[15]:

df = pd.DataFrame()
i = 0
for key in keys:
    i = i+1
    outDict = splitKey(key)
    df2 = pd.DataFrame(outDict,index=[i])
    df = df.append(df2)    


# In[16]:

df["nodata"] = -9999
df["ingested_by"] ="RutgerHofste"
df["exportdescription"] = df["indicator"]


# In[17]:

df_errors = pd.DataFrame()
start_time = time.time()
rows = df.shape[0]*1.0
for index, row in df.iterrows():
    elapsed_time = time.time() - start_time 
    print(index,"%.2f" %((index/rows)*100), "elapsed: ", str(timedelta(seconds=elapsed_time)))
    df_errors2 = uploadEE(index,row)
    df_errors = df_errors.append(df_errors2)


# In[ ]:



