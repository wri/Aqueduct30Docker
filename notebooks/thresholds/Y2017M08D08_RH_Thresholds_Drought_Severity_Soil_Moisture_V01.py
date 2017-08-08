
# coding: utf-8

# # Thresholds Drought Severity Soil Moisture
# 
# * Purpose of script: Double check the threshold setting and categorization for the water stress score of Aqueduct 30
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170808

# In[1]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Convert_Indicators_ASC_Geotiff_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Thresholds_Drought_Severity_Soil_Moisture_V01/output/"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D08_RH_Thresholds_Drought_Severity_Soil_Moisture_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D08_RH_Thresholds_Drought_Severity_Soil_Moisture_V01/output/"


# In[2]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')
get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# ## Categories for Drought Severity Soil Moisture based on Yoshi's Email
# 
# 1. <0.1: low stress
# 2. 0.1<=x<0.25: moderate stress
# 3. 0.25<=x<0.5: mid stress 
# 4. 0.5<=x<0.75: severe stress
# 5. \>0.75 (put 1 as max., don't go to 26....): extremely severe stress
# 

# In[9]:

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
import os
import numpy as np


# In[21]:

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

def categorizeRaster(inputRasterPath,outputRasterPath,bins):
    xsize,ysize,geotransform,geoproj,Z = readFile(inputRasterPath)
    Z_out = np.digitize(Z, bins)
    Z_out[Z_out==0] = -9999
    writeFile(outputRasterPath,geotransform,geoproj,Z_out)



# In[22]:

bins = np.array([0.0,0.1, 0.25, 0.5, 0.75, 1 ,100])


# In[23]:

inputRasterPath = os.path.join(EC2_INPUT_PATH,"global_droughtseveritystandardisedsoilmoisture_5min_1960-2014.tif")
outputRasterPath = os.path.join(EC2_OUTPUT_PATH,"global_droughtseveritystandardisedsoilmoisture_5min_1960-2014_Categorized.tif")


# In[24]:

categorizeRaster(inputRasterPath,outputRasterPath,bins)


# In[25]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# The results are on S3 in bucket :

# In[26]:

print(S3_OUTPUT_PATH)


# In[ ]:



