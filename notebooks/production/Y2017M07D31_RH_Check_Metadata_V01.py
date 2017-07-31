
# coding: utf-8

# # Check metadata
# 
# * Purpose of script: check PCR-GlobWB metadata in a pure pythonic way
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170731

# ## PURPOSE
# 
#     Read University of Utrecht files and store as geotiff
#     this project use a virtual environment with the following pip installs:
#     netcdf4, https://pythongisandstuff.wordpress.com/2016/04/13/installing-gdal-ogr-for-python-on-windows/
# 
# ## PROGRAMMER(S)
#     Rutger Hofste
#     Chris Slocum
# ## REVISION HISTORY
#     20170203 -- GDAL part added
#     20161202 -- Rutger changes to work with UU data
#     20140320 -- Initial version created and posted online
#     20140722 -- Added basic error handling to ncdump
#                 Thanks to K.-Michael Aye for highlighting the issue
# ## REFERENCES
#     netcdf4-python -- http://code.google.com/p/netcdf4-python/
#     colormap -- http://matplotlib.org/examples/pylab_examples/custom_cmap.html
#     GDAL -- https://pythongisandstuff.wordpress.com/2016/04/13/installing-gdal-ogr-for-python-on-windows/
# 
# ## CAVEATS
#     I did not use a robust way to find the filename without extension. Filepath cannot contain periods. No error module built    in.

# In[1]:


try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')


# In[2]:


gdal.UseExceptions()
import datetime as dt
import numpy as np
from netCDF4 import Dataset
import os
import datetime
import math


# In[60]:


NETCDFINPUTPATH = "/volumes/data/PCRGlobWB20V01/waterdemand"
# you can also get the metadata for other folders. You can use the SSH terminal to list the folders. 


PRINT_METADATA = True


# Add Definitions (functions) to the environment

# In[4]:


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


# In[5]:


def normalizeTime(time):
    timeNormal =[]
    for i in range(0, len(time)):
        fullDate = days_since_jan_1_1900_to_datetime(time[i])
        timeNormal.append(fullDate)
    return timeNormal


# In[6]:


def days_since_jan_1_1900_to_datetime(d):
    return datetime.datetime(1900,1,1) +         datetime.timedelta(days=d)


# In[7]:


files = os.listdir(NETCDFINPUTPATH)


# In[8]:


print "number of files: " +str(len(files))


# In[9]:


for oneFile in files:
    netCDFInputFileName = oneFile
    print oneFile
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

    print "Time Minimum: ", min(timeNormal), "Days since start (1901 or 1900)", min(time)
    print "Time Maximum: ", max(timeNormal), "Days since start (1901 or 1900)", max(time)
    print "Number of layers", len(timeNormal)
    
    print min(lats)
    print max(lats)
    print min(lons)
    print max(lons)
    
    


# this script was used to check the metadata. The results were copied to a texteditor and inpected. The results are not saved to disk. 

# Finding the maximum extend of the data to check the NetCDF4 CRS: 
# 
# Latitudes and Longitudes are postings meaning they represent the center of a cell and not the edge. 
# 
# 

# In[59]:


print min(lats) , max(lats), min(lons), max(lons)    


# Number of cells 

# In[12]:


print len(lats)
print len(lons)


# In[16]:


cellsize = 360.0/(len(lons))
cellsize2 = 180.0/(len(lats))
print cellsize, cellsize2


# In[20]:


maxLat = max(lats)+0.5*cellsize
minLat = min(lats)-0.5*cellsize

maxLon = max(lons)+0.5*cellsize
minLon = min(lons)-0.5*cellsize


# The extent has a slight error, caused by the rounding error of the cellsize. This is due to the fact that the model uses 5 arc minute resolution and not a rational number. When creating a reference geotiff, you therefore make a slight error. 

# In[23]:


print maxLat, maxLon
print minLat, minLon


# The error is not significant. For the development of Aqueduct we used a standard geometry and coordinate reference system (CRS). The standard geotiff can be found in our Amazon Bucket:
# 
# `wri-projects/Aqueduct30/rawData/WRI/samplegeotiff`
# 
# ArcGIS has a limited precision when storing CRS and the extent translates to (copied from ArcGIS/QGIS):

# In[24]:


maxLatArc =  090.0000025443094
minLatArc = -089.9999923682499

minLonArc = -179.999994912559
maxLonArc =  179.999994912559


# This yield a maximum error of (Degrees)

# In[57]:


errors = [maxLat-maxLatArc,minLat-minLatArc,maxLon-maxLonArc,minLon-minLonArc]

def absolute(x): 
    return abs(x)

absErrors = map(absolute,errors)
maxError = max(absErrors)
WGS84r = 6378137
circumference = math.pi*WGS84r*2
maxErrorm = maxError * (circumference/360)
print "Maximum error in m: " + str(maxErrorm)


# Which is acceptable given the cell size is 5min or ~9.3km at the equator

# Done

# In[ ]:




