
# coding: utf-8

# In[ ]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

X_DIMENSION_30S = 43200
Y_DIMENSION_30S = 21600

s3_input_path = "s3://wri-projects/Aqueduct30/rawData/WRI/PBL_urban_extents/"


# In[ ]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[ ]:

get_ipython().system('aws s3 cp {s3_input_path} {ec2_input_path} --recursive --exclude="*" --include="*.map"')


# In[11]:

import numpy as np
import aqueduct3
import netCDF4


# In[10]:

def ncdump(nc_fid):
    '''ncdump outputs dimensions, variables and their attribute information.
    -------------------------------------------------------------------------------
    
    The information is similar to that of NCAR's ncdump utility.
    ncdump requires a valid instance of Dataset.

    Args:
        nc_fid (netCDF4.Dataset) : A netCDF4 dateset object

    Returns:
        nc_attrs (list) : A Python list of the NetCDF file global attributes
        nc_dims (list) : A Python list of the NetCDF file dimensions
        nc_vars (list) : A Python list of the NetCDF file variables
    '''

    nc_attrs = nc_fid.ncattrs()
    nc_dims = [dim for dim in nc_fid.dimensions]  # list of nc dimensions
    nc_vars = [var for var in nc_fid.variables]  # list of nc variables
    return nc_attrs, nc_dims, nc_vars


# In[ ]:




# In[7]:

default_geotransform, default_geoprojection = aqueduct3.get_global_georeference(np.ones([Y_DIMENSION_30S,X_DIMENSION_30S]))

#nc_attrs, nc_dims, nc_vars = ncdump(nc_fid)



# In[9]:

nc_fid = netCDF4.Dataset(input_path, 'r')


# In[ ]:



