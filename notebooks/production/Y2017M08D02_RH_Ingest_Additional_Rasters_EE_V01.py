
# coding: utf-8

# # Ingest Additional Rasters on Earth Engine
# 
# * Purpose of script: This notebook will ingest some of the missing rasters to Earth Engine 
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170803

# ## Preparation
# 
# 1. gcloud authorization (`gcloud init`)
# 1. earthengine authorization (`earthengine authorize`)
# 1. aws authorization (`aws configure`)
# 

# In[16]:

get_ipython().system('rm -r /volumes/data/PCRGlobWB20V01/additional')


# In[17]:

get_ipython().system('mkdir /volumes/data/PCRGlobWB20V01/additional')


# In[18]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/WRI/samplegeotiff /volumes/data/PCRGlobWB20V01/additional --recursive')


# Check if the file is actually copied

# In[19]:

get_ipython().system('ls /volumes/data/PCRGlobWB20V01/additional/')


# Copy Indicator files to EC2 instance

# In[20]:

get_ipython().system('mkdir /volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01')


# In[21]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/processData/03PCRGlobWBIndicatorsV01 /volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01 --recursive --quiet')


# Create output folder to store geotiffs

# In[30]:

get_ipython().system('mkdir /volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/output')


# ## Script

# Create working environment and copy relevant files. 

# In[46]:

INPUTLOCATION_SAMPLE_GEOTIFF = "/volumes/data/PCRGlobWB20V01/additional/sampleGeotiff.tiff"
INPUTLOCATION_INDICATORS = "/volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/"
INPUTLOCATION_AUXILIARY = "/volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/auxiliary"
OUTPUTLOCATION_INDICATORS = "/volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/output"
OUTPUTLOCATION_AUX = "/volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/auxiliary/output"
GCS_BASE = "gs://aqueduct30_v01/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/"
GCS_BASE_AUX = "gs://aqueduct30_v01/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01_AUX/"
GCS_BASE_HYBAS = "gs://aqueduct30_v01/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01_HYBAS/"
EE_BASE = "projects/WRI-Aquaduct/PCRGlobWB20V05"


# In[10]:

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


# In[14]:

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
    outDict = {"fileName":fileName,"extension":extension}
    return outDict

def splitFileName(fileName):
    values = re.split("_|-", fileName)
    keys = ["geographic_range","indicator","spatial_resolution","temporal_range_min","temporal_range_max"]
    outDict = dict(zip(keys, values))
    outDict["fileName"] = fileName
    return outDict

def uploadEE(index,row):
    target = EE_BASE + "/" + row.fileName
    source = GCS_BASE + row.fileName + "." + row.extension
    metadata = "--nodata_value=%s -p extension=%s -p filename=%s -p geographic_range=%s -p indicator=%s -p spatial_resolution=%s -p temporal_range_max=%s -p temporal_range_min=%s -p units=dimensionless -p ingested_by=%s -p exportdescription=%s" %(row.nodata,row.extension,row.fileName,row.geographic_range,row.indicator,row.spatial_resolution,row.temporal_range_max,row.temporal_range_min, row.ingested_by, row.exportdescription)
    command = "/opt/anaconda3/bin/earthengine upload image --asset_id %s %s %s" % (target, source,metadata)
    try:
        response = subprocess.check_output(command, shell=True)
        outDict = {"command":command,"response":response,"error":0}
        df_errors2 = pd.DataFrame(outDict,index=[index])
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

# In[19]:

[xsizeSample,ysizeSample,geotransformSample,geoprojSample,ZSample] = readFile(INPUTLOCATION_SAMPLE_GEOTIFF)


# In[50]:

files = os.listdir(INPUTLOCATION_INDICATORS)


# In[53]:

newExtension =".tif"
for oneFile in files:
    if oneFile.endswith(".asc"):
        base , extension = oneFile.split(".")
        xsize,ysize,geotransform,geoproj,Z = readFile(os.path.join(INPUTLOCATION_INDICATORS,oneFile))
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        outputFileName = base + newExtension
        writeFile(os.path.join(OUTPUTLOCATION_INDICATORS,outputFileName),geotransformSample,geoprojSample,Z)
    


# ## Upload PCRGlobWB indicators to GCS

# In[66]:

get_ipython().system('gsutil -m cp {OUTPUTLOCATION_INDICATORS}/*.tif {GCS_BASE}')


# Ingest to earthengine

# In[64]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls %s") %(GCS_BASE)


# In[94]:

keys = subprocess.check_output(command,shell=True)
keys2 = keys.decode('UTF-8').splitlines()


# In[95]:

df = pd.DataFrame()
i = 0
for key in keys2:
    i = i+1
    outDict_key = splitKey(key)
    df2 = pd.DataFrame(outDict_key,index=[i])
    df = df.append(df2)   


# In[96]:

df.head()


# In[107]:

df_fileName = pd.DataFrame()

for index, row in df.iterrows():
    outDict_fileName = splitFileName(row.fileName)
    df2 = pd.DataFrame(outDict_fileName,index=[index])
    df_fileName = df_fileName.append(df2)  


# In[108]:

df_fileName.head()


# In[109]:

df_complete = df.merge(df_fileName,how='left',left_on='fileName',right_on='fileName')


# In[110]:

df_complete.head()


# As you can seem, the structure of the filename is slightly different than the netCDF4 files from Utrecht. The unit for example is not stored int the fileName. 

# In[111]:

df_complete["nodata"] = -9999
df_complete["ingested_by"] ="RutgerHofste"
df_complete["exportdescription"] = df_complete["indicator"]


# In[115]:

df_complete.head()


# In[127]:

df_errors = pd.DataFrame()
start_time = time.time()
for index, row in df_complete.iterrows():
    elapsed_time = time.time() - start_time 
    print(index,"elapsed: ", str(timedelta(seconds=elapsed_time)))
    df_errors2 = uploadEE(index,row)
    df_errors = df_errors.append(df_errors2)


# In[130]:

df_errors


# ## PCRGlobWB auxiliary datasets
# 
# after receiving the results from Yoshi, there was a fair amount of information missing such as flow direction, basins etc. Rens van Beek has been very supportive and provided WRI with some auxiliary datasets. This script will put them in the right format, upload to GCS and ingest them to Earth Engine.  
# 
# Copy relevant datasets to processData folder:
# 

# In[5]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/Utrecht/additionalFiles/flowNetwork/topo_pcrglobwb_05min s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01 --recursive --quiet')


# Copy to EC2 instance

# In[35]:

get_ipython().system('mkdir {INPUTLOCATION_AUXILIARY}')


# In[36]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01 /volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/auxiliary --recursive --quiet')


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
# |37325056| 02-24-2017| 15:44 |  gtopo05min.map| $$km^2$$ |gtopo_05min_km2.map|
# |9331456|  02-24-2017| 15:45 |   lddsound_05min.map| numpad |lddsound_05min_numpad.map |
# |121306624| | |               4 files| | |
# 
# All files are 5 arc minute maps in PCRaster format and WGS84 projection (implicit).
# The gtopo05min.map is the DEM from the gtopo30 dataset that we use for downscaling meteo data occasionally. This is consistent with the CRU climate data sets and the hydro1k drainage dataset. Elevation is in **metres**. The cellsize05.correct.map is the surface area of a geographic cell in **m2** per 5 arc minute cell. The lddsound_05min.map is identical to the LDD we sent you earlier with the **8-point pour algorithm**.(numpad e.g. 7 is NW 6 is E etc.) The accumulated_drainage_area_05min_sqkm.map is the accumulated drainage area in **km2** per cell along the LDD.
# 
# 
# 
# 

# In[37]:

get_ipython().system('mv {INPUTLOCATION_AUXILIARY}/accumulated_drainage_area_05min_sqkm.map {INPUTLOCATION_AUXILIARY}/accumulateddrainagearea_05min_km2.map')
get_ipython().system('mv {INPUTLOCATION_AUXILIARY}/cellsize05min.correct.map {INPUTLOCATION_AUXILIARY}/cellsize_05min_m2.map')
get_ipython().system('mv {INPUTLOCATION_AUXILIARY}/gtopo05min.map {INPUTLOCATION_AUXILIARY}/gtopo_05min_km2.map')
get_ipython().system('mv {INPUTLOCATION_AUXILIARY}/lddsound_05min.map {INPUTLOCATION_AUXILIARY}/lddsound_05min_numpad.map')



# Create output directory for the Auxiliary geotiffs

# In[38]:

get_ipython().system('mkdir {OUTPUTLOCATION_AUX}')


# In[39]:

filesAux = os.listdir(INPUTLOCATION_AUXILIARY)


# In[40]:

newExtension =".tif"
for oneFileAux in filesAux:
    if oneFileAux.endswith(".map"):
        print(oneFileAux)
        base , extension = oneFileAux.split(".")
        xsize,ysize,geotransform,geoproj,Z = readFile(os.path.join(INPUTLOCATION_AUXILIARY,oneFileAux))
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        outputFileName = base + newExtension
        writeFile(os.path.join(OUTPUTLOCATION_AUX,outputFileName),geotransformSample,geoprojSample,Z)
        


# In[41]:

get_ipython().system('ls /volumes/data/Y2017M08D02_RH_Ingest_Additional_Rasters_EE_V01/auxiliary/output')


# Upload Auxiliary files to GCS

# In[43]:

get_ipython().system('gsutil -m cp {OUTPUTLOCATION_AUX}/*.tif {GCS_BASE_AUX}')


# Ingest GCS data in earthengine. Some metadata is missing, therefore 

# In[ ]:

def uploadEEaux(index,row):
    target = EE_BASE_AUX + "/" + row.fileName
    source = GCS_BASE + row.fileName + "." + row.extension
    metadata = "--nodata_value=%s -p extension=%s -p filename=%s -p geographic_range=%s -p indicator=%s -p spatial_resolution=%s -p temporal_range_max=%s -p temporal_range_min=%s -p units=dimensionless -p ingested_by=%s -p exportdescription=%s" %(row.nodata,row.extension,row.fileName,row.geographic_range,row.indicator,row.spatial_resolution,row.temporal_range_max,row.temporal_range_min, row.ingested_by, row.exportdescription)
    command = "/opt/anaconda3/bin/earthengine upload image --asset_id %s %s %s" % (target, source,metadata)
    try:
        response = subprocess.check_output(command, shell=True)
        outDict = {"command":command,"response":response,"error":0}
        df_errors2 = pd.DataFrame(outDict,index=[index])
    except:
        try:
            outDict = {"command":command,"response":response,"error":1}
        except:
            outDict = {"command":command,"response":-9999,"error":2}
        df_errors2 = pd.DataFrame(outDict,index=[index])
        print("error")
    return df_errors2


# In[48]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls %s") %(GCS_BASE_AUX)


# In[49]:

keys = subprocess.check_output(command,shell=True)
keys2 = keys.decode('UTF-8').splitlines()


# In[50]:




# Upload Rasterized HydroBasin files to GCS

# In[47]:

get_ipython().system('gsutil -m cp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/output/*.tif {GCS_BASE_HYBAS}')


# In[ ]:



