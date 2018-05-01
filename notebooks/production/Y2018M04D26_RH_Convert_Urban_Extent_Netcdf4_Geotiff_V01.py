
# coding: utf-8

# In[1]:

""" Convert PBL netcdf4 to geotiff.
-------------------------------------------------------------------------------
Converts a netcdf4 file without time band to geotiff. Does not copy metadata.
Script written for Samantha Kuzma.

Author: Rutger Hofste
Date: 20180426
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    SCRIPT_NAME (string) : Script name.
    S3_INPUT_PATH (string) : Amazon S3 input path. 
    INPUT_VERSION (integer) : Input version.
    INPUT_FILE_NAME (string) : Input file name.
    OUTPUT_FILE_NAME (string) : Output file name
    OUTPUT_VERSION (integer) : Output version.     
    EXPORT_VARIABLE (string) : Variable to export. Must be of type Geo2D.
Returns:

Todo:
replace score with underscre in soil moisture time-series. 

"""

SCRIPT_NAME = "Y2018M04D26_RH_Convert_Urban_Extent_Netcdf4_Geotiff_V01"
S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/WRI/PBL_urban_extents/"
INPUT_VERSION = 1
INPUT_FILE_NAME = "exposure_base_2010_urb.nc"
OUTPUT_FILE_NAME = "exposure_base_2010_urb"
OUTPUT_VERSION = 3
# Hardcode
EXPORT_VARIABLE = "Urban Land Use"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,INPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
output_file_path = "{}/{}_V{:02.0f}.tif".format(ec2_output_path,OUTPUT_FILE_NAME,OUTPUT_VERSION)

print("Input S3: " + S3_INPUT_PATH +
      "\nInput ec2: " + ec2_input_path +
      "\nOutput ec2: " + ec2_output_path +
      "\nOutput S3: " + s3_output_path +
      "\nOutut file path: " + output_file_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[7]:




# In[3]:

import numpy as np
import aqueduct3
import netCDF4
from osgeo import gdal
import matplotlib as plt

def etl():
    get_ipython().system('rm -r {ec2_input_path}')
    get_ipython().system('rm -r {ec2_output_path}')
    get_ipython().system('mkdir -p {ec2_input_path}')
    get_ipython().system('mkdir -p {ec2_output_path}')
    get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


def main():
    etl()
    input_file_path = "{}/{}".format(ec2_input_path,INPUT_FILE_NAME)
    nc_fid = netCDF4.Dataset(input_file_path, 'r')
    nc_attrs, nc_dims, nc_vars = aqueduct3.ncdump(nc_fid)
    print(nc_attrs, nc_dims, nc_vars)
    y_dimension = nc_fid.variables["lat"].shape[0]
    x_dimension = nc_fid.variables["lon"].shape[0]
    default_geotransform, default_geoprojection = aqueduct3.get_global_georeference(np.ones([y_dimension,x_dimension]))
    Z = nc_fid.variables[EXPORT_VARIABLE][:,:]
    Z = np.flipud(Z)
    aqueduct3.write_geotiff(output_file_path,default_geotransform,default_geoprojection,Z,nodata_value=-9999,datatype=gdal.GDT_Int32)
    get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')

if __name__ == "__main__":
    main()


# In[4]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:    
# 0:01:25.589867
