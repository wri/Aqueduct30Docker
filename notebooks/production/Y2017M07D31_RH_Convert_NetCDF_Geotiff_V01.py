
# coding: utf-8

# # Check metadata
# 
# * Purpose of script: This notebook will transform netCDF to geotiff.  
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170731

# as explained in the previous step, we need to put the NetCDF data into a coordinate reference system. For that we use a standard geometry. 
# 
# Download the standard geotiff to our instance:

# # Preparation

# Run the following command just in case an old folder exists. _If no older folder exist you will get the error: rm: cannot remove '/volumes/data/PCRGlobWB20V01/additional/*': No such file or directory_

# # Settings

# In[1]:

EC2_INPUT_PATH_ADDITIONAL = "/volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V01/input"
S3_INPUT_PATH_ADDITIONAL = "s3://wri-projects/Aqueduct30/rawData/WRI/samplegeotiff/"
EC2_INPUT_PATH = "/volumes/data/Y2017M07D31_RH_download_PCRGlobWB_data_V01/output/"
PRINT_METADATA = False
EC2_OUTPUTPATH = "/volumes/data/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V01/output/"


# In[2]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH_ADDITIONAL}')


# In[3]:

get_ipython().system('mkdir -p {EC2_OUTPUTPATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_ADDITIONAL} {EC2_INPUT_PATH_ADDITIONAL} --recursive')


# Check if the file is actually copied

# In[5]:

get_ipython().system('ls {EC2_INPUT_PATH_ADDITIONAL}')


# In[6]:

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
    
from netCDF4 import Dataset
import os
import datetime
import subprocess


# In[7]:

inputLocationSampleGeotiff = os.path.join(EC2_INPUT_PATH_ADDITIONAL,"sampleGeotiff.tiff")


# In[8]:

print(inputLocationSampleGeotiff)


# # Functions

# In[9]:

def netCDF4toGeotiff(fileName,fileLocation):
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

# In[10]:

[xsize,ysize,geotransform,geoproj,ZSample] = readFile(inputLocationSampleGeotiff)


# These are the parameters of the standard geometry. 

# In[11]:

print xsize, ysize, geotransform


# In[12]:

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



