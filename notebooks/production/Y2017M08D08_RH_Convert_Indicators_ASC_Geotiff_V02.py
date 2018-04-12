
# coding: utf-8

# In[33]:

""" Convert Indicators from ASCII to Geotiff
-------------------------------------------------------------------------------
A couple of indicators are shared in ASCII format. Converting to geotiff and
upload to GCS and AWS.


Author: Rutger Hofste
Date: 20170808
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    S3_INPUT_PATH (string) : S3 input path containing the ascii rasters.  
    GCS_OUTPUT (string) : Google Cloud Storage output namespace.

    X_DIMENSION_5MIN (integer) : horizontal or longitudinal dimension of 
                                 raster.
    Y_DIMENSION_5MIN (integer) : vertical or latitudinal dimension of 
                                 raster.
    



Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V02"


INPUT_VERSION = 2
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M07D31_RH_copy_S3raw_s3process_V{:02.0f}/output/".format(INPUT_VERSION)


GCS_OUTPUT = "gs://aqueduct30_v01/{}/".format(SCRIPT_NAME)


X_DIMENSION_5MIN = 4320
Y_DIMENSION_5MIN = 2160

# Output Parameters


# In[4]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[31]:

# imports
import os
import numpy as np
from osgeo import gdal
import aqueduct3


# In[ ]:

# ETL


# In[35]:

ec2_input_path =  "/volumes/data/{}/input/".format(SCRIPT_NAME)
ec2_output_path = "/volumes/data/{}/output/".format(SCRIPT_NAME)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output/".format(SCRIPT_NAME)


# In[7]:

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[11]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive --exclude="*" --include="*.asc"')


# In[32]:

# Convert the ascii files in the ec2_input_directory into geotiffs in the ec2_output_directory

default_geotransform, default_geoprojection = aqueduct3.get_global_georeference(np.ones([Y_DIMENSION_5MIN,X_DIMENSION_5MIN]))
for root, dirs, file_names in os.walk(ec2_input_path):
    for file_name in file_names:
        print(file_name)
        base , extension = file_name.split(".")
        output_path = ec2_output_path  + base + ".tif"
        input_path = os.path.join(root, file_name)     

        xsize,ysize,geotransform,geoproj,Z = aqueduct3.read_gdal_file(input_path)
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        aqueduct3.write_geotiff(output_path,default_geotransform,default_geoprojection,Z,nodata_value=-9999,datatype=gdal.GDT_Float32)         



# Upload to GCS

# In[34]:

get_ipython().system('gsutil -m cp {ec2_output_path}/*.tif {GCS_OUTPUT}')


# In[36]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[39]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[37]:

command = ("/opt/google-cloud-sdk/bin/gsutil ls %s") %(GCS_OUTPUT)


# In[38]:

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

