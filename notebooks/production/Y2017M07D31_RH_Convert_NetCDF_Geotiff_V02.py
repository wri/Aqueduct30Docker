
# coding: utf-8

# In[ ]:

""" convert netCDF4 to Geotiff.
-------------------------------------------------------------------------------

Convert individual images from a netCDF to geotiffs. Output is stored in 
Amazon S3 folder. 


Author: Rutger Hofste
Date: 20180327
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    EC2_INPUT_PATH (string) : path to output of previous script. See Readme 
                              for more details. 
    PRINT_METADATA (boolean) : Print out metadata in Jupyter Notebook


Returns:

"""

# Input Parameters

SCRIPT_NAME = "Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02"

EC2_INPUT_PATH = "/volumes/data/Y2017M07D31_RH_download_PCRGlobWB_data_V02/output/"

PRINT_METADATA = False

# Output Parameters


# In[ ]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[ ]:

# Imports
import os
import datetime
import subprocess
import numpy as np
import pyproj
import warnings
from netCDF4 import Dataset

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')

if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'



# In[ ]:

# ETL

ec2_input_path_additional = "/volumes/data/{}/input".format(SCRIPT_NAME)

S3_INPUT_PATH_ADDITIONAL = "s3://wri-projects/Aqueduct30/rawData/WRI/samplegeotiff/"

ec2_output_path = "/volumes/data/{}/output/".format(SCRIPT_NAME)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output/".format(SCRIPT_NAME)


# In[ ]:







# In[ ]:

output_path = "/volumes/data/{}/output/test4.tif"


# In[ ]:

x_dimenstion_5min = 4320
y_dimension_5min = 2160


# In[ ]:

array = np.ones([y_dimension,x_dimenstion])


# In[ ]:

test = create_global_geotiff(output_path,array)


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

get_ipython().system('mkdir -p {ec2_input_path_additional}')


# In[ ]:

get_ipython().system('mkdir -p {EC2_OUTPUTPATH}')


# In[ ]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_ADDITIONAL} {ec2_input_path_additional} --recursive')


# Check if the file is actually copied

# In[ ]:

get_ipython().system('ls {EC2_INPUT_PATH_ADDITIONAL}')


# In[ ]:

inputLocationSampleGeotiff = os.path.join(ec2_input_path_additional,"sampleGeotiff.tiff")


# In[ ]:

print(inputLocationSampleGeotiff)


# In[ ]:




# # Functions

# In[ ]:

# Functions
def create_global_geotiff_from_array(output_path,array,datatype=gdal.GDT_Int16,nodata_value=-9999):
    """ Writes array to global geotiff
    -------------------------------------------------------------------------------
    
    In many geospatial debugging analyses, a raster with only ones or random values
    is useful. Stores the output geotiff in the output_path. 
        
    Uses the WGS epsg 4326 projection. Make sure the array and data type are 
    compatible. Requires GDAL to be installed with GDAL_DATA added to the system
    path. 
    
    Args:
        output_path (string) : file path with write permission to store geotiff.
        array (np.array) : numpy array to write to geotiff.
        datatype (gdal datatype) : datatype of output image. See https://naturalatlas.github.io/node-gdal/classes/Constants%20(GDT).html
                                   for options. Defaults to gdal.GDT_Float32
        nodata_value (integer) : NoData value. Defaults to -9999.
    
    Returns:
        image (geoTiff) : geotiff image with all
        
    
    TODO: Support other crs
    
    """
    y_dimension = array.shape[0] #rows, lat
    x_dimension = array.shape[1] #cols, lon
    geotransform = (-180,360.0/x_dimension,0,90,0,-180.0/y_dimension)
    
    driver = gdal.GetDriverByName('GTiff')
    out_raster = driver.Create(output_path, x_dimension, y_dimension, 1, datatype,['COMPRESS=LZW'])
    out_raster.SetGeoTransform(geotransform)
    out_band = out_raster.GetRasterBand(1)
    out_band.SetNoDataValue(nodata_value)
    out_band.WriteArray(array)
    out_raster_srs = osr.SpatialReference()
    out_raster_srs.ImportFromEPSG(4326)
    
    projection = out_raster_srs.ExportToWkt()
    if len(projection) == 0:
        warnings.warn("GDAL_DATA path not set correctly. Assert os.environ "                       "contains GDAL_DATA \n"                       "Code will execute without projection set")
    
    out_raster.SetProjection(projection)
    out_band.FlushCache()        
    return out_raster


# In[ ]:


def netCDF4_to_geotiff(fileName,fileLocation):
    netCDFInputBaseName = fileName.split('.')[0]
    nc_fid = Dataset(fileLocation, 'r')
    nc_attrs, nc_dims, nc_vars = ncdump(nc_fid, PRINT_METADATA)
    parameter = nc_vars[3]
    lats = nc_fid.variables['latitude'][:]  # extract/copy the data
    lons = nc_fid.variables['longitude'][:]
    times = nc_fid.variables['time'][:]
    timeUnit = nc_fid.variables["time"].getncattr("units")
    timeNormal =[]
    for time in times:
        if timeUnit == ("days since 1900-01-01 00:00:00") or (timeUnit =="Days since 1900-01-01"):
            timeNormal.append(datetime.datetime(1900,1,1) + datetime.timedelta(days=time))
        elif timeUnit == "days since 1901-01-01 00:00:00":
            timeNormal.append(datetime.datetime(1901,1,1) + datetime.timedelta(days=time))
        else:
            print "Error"
            timeNormal.append(-9999)
            
    for i in range(0,len(timeNormal)):
        #print timeNormal[i].year
        Z = nc_fid.variables[parameter][i, :, :]
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        outputFilename = netCDFInputBaseName + "I%0.3dY%0.2dM%0.2d.tif" %(i,timeNormal[i].year,timeNormal[i].month)
        writefilename = os.path.join(EC2_OUTPUTPATH,outputFilename)
        writeFile(writefilename,geotransform,geoproj,Z)
    
    return time, timeUnit, timeNormal

def read_file(input_path):
    """ Reads file using GDAL
    
    Args:
        input_path (string) : path to input file
    
    Returns:
        xsize (integer) : number of columns
        ysize (integer) : number of rows
    
    """
    
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

def ncdump(nc_fid, verb=True):
    '''
    ncdump outputs dimensions, variables and their attribute information.
    The information is similar to that of NCAR's ncdump utility.
    ncdump requires a valid instance of Dataset.

    Parameters
    ----------
    nc_fid : netCDF4.Dataset
        A netCDF4 dateset object
    verb : Boolean
        whether or not nc_attrs, nc_dims, and nc_vars are printed

    Returns
    -------
    nc_attrs : list
        A Python list of the NetCDF file global attributes
    nc_dims : list
        A Python list of the NetCDF file dimensions
    nc_vars : list
        A Python list of the NetCDF file variables
    '''
    def print_ncattr(key):
        """
        Prints the NetCDF file attributes for a given key

        Parameters
        ----------
        key : unicode
            a valid netCDF4.Dataset.variables key
        """
        try:
            print "\t\ttype:", repr(nc_fid.variables[key].dtype)
            for ncattr in nc_fid.variables[key].ncattrs():
                print '\t\t%s:' % ncattr,                      repr(nc_fid.variables[key].getncattr(ncattr))
        except KeyError:
            print "\t\tWARNING: %s does not contain variable attributes" % key

    # NetCDF global attributes
    nc_attrs = nc_fid.ncattrs()
    if verb:
        print "NetCDF Global Attributes:"
        for nc_attr in nc_attrs:
            print '\t%s:' % nc_attr, repr(nc_fid.getncattr(nc_attr))
    nc_dims = [dim for dim in nc_fid.dimensions]  # list of nc dimensions
    # Dimension shape information.
    if verb:
        print "NetCDF dimension information:"
        for dim in nc_dims:
            print "\tName:", dim
            print "\t\tsize:", len(nc_fid.dimensions[dim])
            print_ncattr(dim)
    # Variable information.
    nc_vars = [var for var in nc_fid.variables]  # list of nc variables
    if verb:
        print "NetCDF variable information:"
        for var in nc_vars:
            if var not in nc_dims:
                print '\tName:', var
                print "\t\tdimensions:", nc_fid.variables[var].dimensions
                print "\t\tsize:", nc_fid.variables[var].size
                print_ncattr(var)
    return nc_attrs, nc_dims, nc_vars


# # Script

# In[ ]:




# In[ ]:

type(ZSample)


# In[ ]:

print xsize, ysize, geotransform


# In[ ]:

for root, dirs, files in os.walk(EC2_INPUT_PATH):
    for oneFile in files:
        if oneFile.endswith(".nc4") or oneFile.endswith(".nc"):
            print(oneFile)
            fileLocation = os.path.join(root, oneFile)
            fileName = oneFile
            netCDF4toGeotiff(fileName,fileLocation)
                


# In[ ]:

files = os.listdir(OUTPUTPATH)
print("Number of files: " + str(len(files)))


# Some files from Utrecht contain double years, removing the erroneous ones (used Panoply/Qgis to inspect those files):
# 
# global_historical_PDomWN_year_millionm3_5min_1960_2014I055Y1960M01.tif
# global_historical_PDomWN_month_millionm3_5min_1960_2014I660Y1960M01.tif
# global_historical_PDomWN_month_millionm3_5min_1960_2014I661Y1960M01.tif
# 
# 
# 

# In[ ]:

get_ipython().system('mkdir /volumes/data/trash')


# In[ ]:

get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V01/global_historical_PDomWN_year_millionm3_5min_1960_2014I055Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_year_millionm3_5min_1960_2014I055Y1960M01.tif')
get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V01/global_historical_PDomWN_month_millionm3_5min_1960_2014I660Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_month_millionm3_5min_1960_2014I660Y1960M01.tif')
get_ipython().system('mv /volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V01/global_historical_PDomWN_month_millionm3_5min_1960_2014I661Y1960M01.tif /volumes/data/trash/global_historical_PDomWN_month_millionm3_5min_1960_2014I661Y1960M01.tif')



# In[ ]:

files = os.listdir(OUTPUTPATH)
print("Number of files: " + str(len(files)))


# In[ ]:



