""" Aqueduct Function Codebase
-------------------------------------------------------------------------------
DO NOT MOVE/RENAME THIS FILE

frequently used functions for Aqueduct Jupyter Notebooks


Author: Rutger Hofste
Date: 20180329
Kernel: N/A
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

# Imports
import os
import datetime
import netCDF4
import subprocess
import pandas as pd

try:
    from osgeo import ogr, osr, gdal
except:
    sys.exit('ERROR: cannot find GDAL/OGR modules')

if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'

def checks():
    """ Check if GDAL DATA is configured correctly.
    -------------------------------------------------------------------------------
    
    Args:
        None
        
    Returns:
        geoprojection (string) : The standard epsg 4326 projection.
    
    """
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    geoprojection = srs.ExportToWkt()    
    return geoprojection



def read_gdal_file(input_path):
    """ Reads file using GDAL
    -------------------------------------------------------------------------------
    
    WARNING: This function only reads the first band. Data Stored in memory
    
    Args:
        input_path (string) : path to input file
    
    Returns:
        xsize (integer) : number of columns
        ysize (integer) : number of rows
        geotransform (tuple) : geotransform
        geoproj (string) : geoprojection in osr format
        Z (np.array) : array with values 
    
    """
    
    filehandle = gdal.Open(input_path)
    band1 = filehandle.GetRasterBand(1)
    geotransform = filehandle.GetGeoTransform()
    geoproj = filehandle.GetProjection()
    Z = band1.ReadAsArray()
    xsize = filehandle.RasterXSize
    ysize = filehandle.RasterYSize
    filehandle = None
    return xsize,ysize,geotransform,geoproj,Z



def get_global_georeference(array):
    """ Get the geotransform and projection for a numpy array
    -------------------------------------------------------------------------------
    
    Returns a geotransform and projection for a global extent in epsg 4326 
    projection.
    
    Args:
        array (np.array) : numpy array
    
    Returns:
        geotransform (tuple) : geotransform
        geoprojection (string) : geoprojection in osr format    
    
    """
    
    y_dimension = array.shape[0] #rows, lat
    x_dimension = array.shape[1] #cols, lon
    geotransform = (-180,360.0/x_dimension,0,90,0,-180.0/y_dimension)
    
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    geoprojection = srs.ExportToWkt()
    
    if len(geoprojection) == 0:
        warnings.warn("GDAL_DATA path not set correctly. Assert os.environ " \
                      "contains GDAL_DATA \n" \
                      "Code will execute without projection set")

    return geotransform, geoprojection


def write_geotiff(output_path,geotransform,geoprojection,data,nodata_value=-9999,datatype=gdal.GDT_Float32):
    
    """ Write data to geotiff file
    -------------------------------------------------------------------------------
    
    Args: 
        output_path (string) : output_path 
        geotransform (tuple) : geotransform
        geoprojection (string) : geoprojection in osr format
        data (np.array) : numpy array    
        nodata_value (integer) : NoData value
        datatype (GDAL datatype)
    
    """  
    
    (x,y) = data.shape
    format = "GTiff"
    driver = gdal.GetDriverByName(format)
    # you can change the dataformat but be sure to be able to store negative values including -9999
    dst_ds = driver.Create(output_path,y,x,1,datatype, [ 'COMPRESS=LZW' ])
    dst_ds.GetRasterBand(1).SetNoDataValue(nodata_value)
    dst_ds.GetRasterBand(1).WriteArray(data)
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(geoprojection)
    dst_ds = None
    return 1


def netCDF4_to_geotiff(file_name,input_path,output_dir_path, output_geotransform, output_geoprojection):
    """Convert every image in a netCDF4 file to a geotiff
    -------------------------------------------------------------------------------
    
    the output filenames will be appended with relevant metadata stored in the 
    netCDF file. 
    
    
    Args:
        file_name (string) : filename including extension.
        input_path (string) : input path to netCDF4 file (.nc or .nc4)
        output_dir_path (string) : output path to directory (.tif or .tiff)
        output_geotransform (tuple) : geotransform
        output_geoprojection (string) : geoprojection in osr format
        
    Returns    
    
    """
    
    netCDF_input_base_name = file_name.split('.')[0]
    nc_fid = netCDF4.Dataset(input_path, 'r')
    nc_attrs, nc_dims, nc_vars = ncdump(nc_fid)
    parameter = nc_vars[3]
    
    lats = nc_fid.variables['latitude'][:]  # extract/copy the data
    lons = nc_fid.variables['longitude'][:]
    times = nc_fid.variables['time'][:]
    time_unit = nc_fid.variables["time"].getncattr("units")
    
    standardized_time = standardize_time(time_unit,times)

      
    for i in range(0,len(standardized_time)):
        Z = nc_fid.variables[parameter][i, :, :]
        Z[Z<-9990]= -9999
        Z[Z>1e19] = -9999
        output_filename = netCDF_input_base_name + "I{:03.0f}Y{:04.0f}M{:02.0f}.tif".format(i,standardized_time[i].year,standardized_time[i].month)
        output_path = os.path.join(output_dir_path,output_filename)
        #writeFile(writefilename,geotransform,geoproj,Z)
        print(output_path)
        write_geotiff(output_path,output_geotransform,output_geoprojection,Z,nodata_value=-9999,datatype=gdal.GDT_Float32)
    
    return Z


def standardize_time(time_unit,times):
    """ Append standardize time to list
    -------------------------------------------------------------------------------
    
    The netCDF results of the university of Utrecht consist of multiple time 
    formats. 
    
    Args:
        time_unit (string) : units as provided by the netCDF4 file. 
        times (list) : list of time in units provided in time_units (e.g. days).
    
    Returns:
        standardized_time (list) : list of normalized times in datetime format.
    
    """
    
    standardized_time =[]
    for time in times:
        if time_unit == ("days since 1900-01-01 00:00:00") or (time_unit =="Days since 1900-01-01"):
            standardized_time.append(datetime.datetime(1900,1,1) + datetime.timedelta(days=time))
        elif time_unit == "days since 1901-01-01 00:00:00":
            standardized_time.append(datetime.datetime(1901,1,1) + datetime.timedelta(days=time))
        else:
            raise("Error, unknown format:",time_unit)
            standardized_time.append(-9999)
    return standardized_time
    
    
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



def upload_geotiff_to_EE_imageCollection(geotiff_gcs_path,output_ee_asset_id,properties,index):
    """Upload geotiff to earthengine image collection
    -------------------------------------------------------------------------------
    
    Ingest a geotiff to earth engine imageCollection and set metadata. A dictionary
    of properties will be used to define the metadata of the image.
    
    Args:
        geotiff_gcs_path (string) : Google Cloud Storage path of geotiff.
        output_ee_asset_id (string) : Earth Engine output asset id. Full path 
                                      including imageCollection asset id.
        properties (dictionary) : Dictionary with metadata. the 'nodata_value' key
                                  can be used to set a NoData Value.
        index (object) : integer or string with the index used for capturing the
                         error dataframe.
        
    
    Returns:
        df_errors2 (pd.Dataframe) : Pandas DataFrame with the command and response.
        
    
    TODO:
    update function to work with dictionary of properties
    
    """
    command = "/opt/anaconda3/bin/earthengine upload image --asset_id {} {}".format(output_ee_asset_id,geotiff_gcs_path)
    metadata_command = dictionary_to_EE_upload_command(properties)
    
    command = command + metadata_command
    
    
    try:
        response = subprocess.check_output(command, shell=True)
        out_dict = {"command":command,"response":response,"error":0}
        df_errors2 = pd.DataFrame(out_dict,index=[index])
        pass
    except:
        try:
            out_dict = {"command":command,"response":response,"error":1}
        except:
            out_dict = {"command":command,"response":-9999,"error":2}
        df_errors2 = pd.DataFrame(out_dict,index=[index])
        print("error")
    return df_errors2

def dictionary_to_EE_upload_command(d):
    """ Convert a dictionary to command that can be appended to upload command
    -------------------------------------------------------------------------------
    WARNING: images with temporal resolution 'year' will have a month 12 
    stored in their metadata. This is for convenience (equal string length). The
    result is an odd looking time_start property of 'yyyy-12-01' for yearly images.     
    
    Args:
        d (dictionary) : Dictionary with metadata. nodata_value, 
                         temporal_resolution are used as special properties.
    
    Returns:
        command (string) : string to append to upload string.    
    
    """
    command = ""
    # Add start and end timestamp
    command = command + " --time_start {:04.0f}-{:02.0f}-01".format(d["year"],d["month"])
    
    for key, value in d.items():
            
        if key == "nodata_value":
            command = command + " --nodata_value={}".format(value)
        else:
            command = command + " -p {}={}".format(key,value)

    return command

def create_imageCollection(ic_id):
    """ Creates an imageCollection using command line
    -------------------------------------------------------------------------------
    Args:
        ic_id (string) : asset_id of image Collection.
        
    Returns:
        command (string) : command parsed to subprocess module 
        result (string) : subprocess result 
        
    """
    command = "earthengine create collection {}".format(ic_id)
    result = subprocess.check_output(command,shell=True)
    return command, result

