
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

# In[1]:

get_ipython().system('rm -r /volumes/data/PCRGlobWB20V01/additional/*')


# In[2]:

get_ipython().system('aws s3 sync s3://wri-projects/Aqueduct30/rawData/WRI/samplegeotiff/ /volumes/data/PCRGlobWB20V01/additional/')


# Check if the file is actually copied

# In[3]:

get_ipython().system('rm -r /volumes/data/temp*')


# In[4]:

get_ipython().system('mkdir /volumes/data/temp')


# In[5]:

get_ipython().system('ls /volumes/data/PCRGlobWB20V01/additional/')


# In[25]:

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')
    
from netCDF4 import Dataset
import os
import datetime
import subprocess


# # Functions

# In[7]:

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

def normalizeTime(time):
    timeNormal =[]
    for i in range(0, len(time)):
        if nc_fid.variables["time"].getncattr("units") == "Days since 1900-01-01":
            fullDate = days_since_jan_1_1900_to_datetime(time[i])
        elif nc_fid.variables["time"].getncattr("units") == "Days since 1901-01-01":
            fullDate = days_since_jan_1_1901_to_datetime(time[i])
        else:
            print "Error"
        fullDate = days_since_jan_1_1900_to_datetime(time[i])
        timeNormal.append(fullDate)
    return timeNormal

def days_since_jan_1_1900_to_datetime(d):
    return datetime.datetime(1900,1,1) + datetime.timedelta(days=d)

def days_since_jan_1_1901_to_datetime(d):
    return datetime.datetime(1901,1,1) + datetime.timedelta(days=d)

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
            print '\t\t%s:' % ncattr,                  repr(nc_fid.variables[key].getncattr(ncattr))
    except KeyError:
        print "\t\tWARNING: %s does not contain variable attributes" % key
        
def netCDFtoGeotiff(oneFile):
    netCDFInputFileName = oneFile
    print(oneFile)
    netCDFInputBaseName = netCDFInputFileName.split('.')[0]

    nc_f = os.path.join(NETCDFINPUTPATH,netCDFInputFileName)
    nc_fid = Dataset(nc_f, 'r')  # Dataset is the class behavior to open the file
         # and create an instance of the ncCDF4 class
    nc_attrs, nc_dims, nc_vars = ncdump(nc_fid, PRINT_METADATA)
    parameter = nc_vars[3]

    lats = nc_fid.variables['latitude'][:]  # extract/copy the data
    lons = nc_fid.variables['longitude'][:]
    time = nc_fid.variables['time'][:]
    timeNormal = normalizeTime(time)
    
    for i in range(0,len(timeNormal)):
        print timeNormal[i].year
        Z = nc_fid.variables[parameter][i, :, :]
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        outputFilename = netCDFInputBaseName + "I%0.3dY%0.2dM%0.2d.tif" %(i,timeNormal[i].year,timeNormal[i].month)
        writefilename = os.path.join(OUTPUTPATH,outputFilename)
        writeFile(writefilename,geotransform,geoproj,Z)


# # Script

# In[21]:

NETCDFINPUTPATH = "/volumes/data/PCRGlobWB20V01/"


# In[9]:

PRINT_METADATA = False


# In[10]:

OUTPUTPATH = "/volumes/data/temp"


# In[11]:

inputLocationSampleGeotiff = "/volumes/data/PCRGlobWB20V01/additional/sampleGeotiff.tiff"


# In[12]:

[xsize,ysize,geotransform,geoproj,ZSample] = readFile(inputLocationSampleGeotiff)


# These are the parameters of the standard geometry. 

# In[13]:

print xsize, ysize, geotransform


# Specify if you want to print metadata. This is similar to the previous step and might be redundant. 

# Copy PLivWN to PLivWW because Livestock Withdrawal = Livestock Consumption (see Yoshi's email'). This will solve some lookping issues in the future. 

# Copies 4GB of data so takes a while

# In[19]:

get_ipython().system('cp /volumes/data/PCRGlobWB20V01/waterdemand/global_historical_PLivWN_month_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/waterdemand/global_historical_PLivWW_month_millionm3_5min_1960_2014.nc4')


# In[20]:

get_ipython().system('cp /volumes/data/PCRGlobWB20V01/waterdemand/global_historical_PLivWN_year_millionm3_5min_1960_2014.nc4 /volumes/data/PCRGlobWB20V01/waterdemand/global_historical_PLivWW_year_millionm3_5min_1960_2014.nc4')


# In[28]:

for root, dirs, files in os.walk(NETCDFINPUTPATH):
    for file in files:
        if file.endswith(".nc4"):
            oneFile = os.path.join(root, file)
                


# In[ ]:

a =nc_fid.variables["time"].getncattr("units")


# In[ ]:

print(OUTPUTPATH)


# In[24]:




# In[ ]:



