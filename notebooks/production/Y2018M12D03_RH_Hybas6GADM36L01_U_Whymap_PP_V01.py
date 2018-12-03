
# coding: utf-8

# In[58]:

""" Union of hydrobasingadm36L01 and Whymap using geopandas parallel processing.
-------------------------------------------------------------------------------

TODO tomorrow: Simplify geometry of large input data, or snap geometries before
performing the union.

Avoid topology errors.


Step 1:
Create polygons (10x10 degree, 648).

Step 2:
Clip all geodataframes with polygons (intersect).

Step 3:
Peform union per polygon.

Step 4: 
Merge results into large geodataframe.

Step 5:
Dissolve on unique identifier.

Step 6:
Save output

Author: Rutger Hofste
Date: 20181203
Kernel: python35+
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = "Y2018M12D03_RH_Hybas6GADM36L01_U_Whymap_PP_V01"
OUTPUT_VERSION = 1

TOLERANCE = 0.000001 # degrees

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_INPUT_TABLE_RIGHT = "y2018m11d14_rh_whymap_to_rds_v01_v01"

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_Merge_V01/output_V06/"
INPUT_FILE_NAME = "Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_Merge_V01.pkl"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path_df = "/volumes/data/{}/outputdf_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path_df = "s3://wri-projects/Aqueduct30/processData/{}/outputdf_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)


print("\nec2_output_path:", ec2_output_path,
      "\ns3_output_path: ", s3_output_path,
      "\ns3_output_path: ", s3_output_path,
      "\nS3_INPUT_PATH: ", S3_INPUT_PATH,
      "\nRDS_INPUT_TABLE_RIGHT: ",RDS_INPUT_TABLE_RIGHT)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[37]:

import os
import sqlalchemy
import multiprocessing
import numpy as np
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import snap


# In[4]:

#r {ec2_output_path}
#!rm -r {ec2_output_path}
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path_df}')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_input_path} --recursive')


# In[6]:

cpu_count = multiprocessing.cpu_count()
cpu_count = cpu_count -2 #Avoid freeze
print("Power to the maxxx:", cpu_count)


# In[7]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[8]:

cell_size = 10


# In[9]:

def create_fishnet_gdf(cell_size):
    crs = {'init': 'epsg:4326'}
    lats = np.arange(-90,90,cell_size)
    lons = np.arange(-180,180,cell_size)
    geoms = []
    for lat in lats:
        for lon in lons:
            llcr = (lon,lat)
            lrcr = (lon+cell_size, lat)
            urcr = (lon+cell_size, lat+cell_size)
            ulcr = (lon, lat+ cell_size)
            geom = Polygon([llcr,lrcr,urcr,ulcr])
            geoms.append(geom)
    gs = gpd.GeoSeries(geoms)
    gdf_grid = gpd.GeoDataFrame(geometry=gs)
    gdf_grid.crs = crs
    return gdf_grid

def post_process_geometry(geometry):
    """ Post Process Shapely Geometries after Intersection
    
    Shapely does not always create the desired output geometry. When
    vertices overlap, the result can be a geometryCollection with
    (mutli)polygons and LineStrings or Points. 
    
    This function converts the results of an intersection. It will
    remove empty geometries
    
    Args: 
        SIMPLIFY_TOLERANCE(double): Global parameter to specify 
            simplification tolerance.
        geomerty (shapely object): GeometryCollection, Multipolygon
            Polygon, Linestring etc.
            
    Returns:
        geometry_out(shapely object): MultiPolygon or
            Polygon of simplified geometry.
            
    Usage:
        apply to geodataframe geometry column.
    
    """
    geometry_buffered = geometry.buffer(0)
    return geometry_buffered


def clip_gdf(gdf_in,polygon):
    """
    Clip geodataframe using shapely polygon.
    Make sure crs is compatible.
    
    Removes any geometry that is (multi)polygon. i.e. LineStrings and Points are Removed
    
    Args:
        gdf (GeoDataFrame): GeoDataFrame in.
        polygon (Shapely Polygon): Polygon used for clipping
    
    """
    crs = gdf_in.crs
    gdf_intersects = gdf_in.loc[gdf_in.geometry.intersects(polygon)]
    df_intersects = gdf_intersects.drop(columns=[gdf_intersects.geometry.name])
    gs_intersections = gpd.GeoSeries(gdf_intersects.geometry.intersection(polygon),crs=crs)
    gdf_clipped = gpd.GeoDataFrame(df_intersects,geometry=gs_intersections)  
    
    # Some clipping results in GeometryCollections with polygons and LineStrings or Points. Convert valid geometry to Multipolygon
    gdf_clipped.geometry = gdf_clipped.geometry.apply(post_process_geometry)
    gdf_clipped_valid = gdf_clipped.loc[gdf_clipped.geometry.is_empty == False]
    return gdf_clipped_valid


def create_union_gdfs(gdf):
    index = gdf.index[0]
    print("Processing: ", index)
    df_out = pd.DataFrame()
    start = datetime.datetime.now()
    polygon = gdf.iloc[0].geometry
    destination_path = "{}/gdf_union_{}.pkl".format(ec2_output_path,index)
    
    gdf_left_clipped= clip_gdf(gdf_left,polygon)
    gdf_right_clipped = clip_gdf(gdf_right,polygon)
    
    if gdf_left_clipped.shape[0] == 0 and gdf_right_clipped.shape[0] == 0:

        gdf_out = None
        write_output = False
    elif gdf_left_clipped.shape[0] != 0 and gdf_right_clipped.shape[0] == 0:
        gdf_out = gdf_left_clipped
        write_output = True
    elif gdf_left_clipped.shape[0] == 0 and gdf_right_clipped.shape[0] != 0:
        gdf_out = gdf_right_clipped
        write_output = True
    elif gdf_left_clipped.shape[0] != 0 and gdf_right_clipped.shape[0] != 0:        
        gdf_union = gpd.overlay(gdf_left_clipped,gdf_right_clipped,how="union")
        gdf_out = gdf_union
        write_output = True        
    
    
    if write_output:
        if TESTING:
            end = datetime.datetime.now()
            elapsed = end - start
            gdf_out["time_processed"] = elapsed.total_seconds()
            gdf_out["tile_index"] = index
        else:
            pass
        gdf_out.to_pickle(path=destination_path)
    else:
        pass
    
    print("Succesfully processed", index)
    return  gdf_out


# In[10]:

gdf_grid = create_fishnet_gdf(cell_size)


# In[11]:

gdf_grid.head()


# In[64]:

gdf_grid.shape


# In[65]:

input_path = "{}/{}".format(ec2_input_path,INPUT_FILE_NAME)


# In[66]:

gdf_left = pd.read_pickle(input_path)


# In[67]:

gdf_left.head()


# In[68]:

gdf_left.shape


# In[69]:

def simplify_gdf(gdf):
    gdf_out = gdf
    gdf_out.geometry = gdf_out.geometry.simplify(tolerance=TOLERANCE)
    return gdf_out


# In[70]:

gdf_left_list = np.array_split(gdf_left, cpu_count*100)


# In[74]:

p= multiprocessing.Pool(processes=cpu_count)
df_out_list = p.map(simplify_gdf,gdf_left_list)
p.close()
p.join()


# In[75]:

gdf_left_simplified = pd.concat(df_out_list, ignore_index=True)


# In[77]:

sql = """
SELECT
  aqid,
  geom
FROM
  {}
""".format(RDS_INPUT_TABLE_RIGHT)


# In[78]:

gdf_right = gpd.read_postgis(sql=sql,
                             con=engine)


# In[79]:

gdf_right.head()


# In[80]:

gdf_right.shape


# In[81]:

# got error index 265 (intersection)


# In[82]:

gdf_grid_test = gdf_grid[265:266]


# In[83]:

polygon = gdf_grid_test.iloc[0].geometry


# In[84]:

gdf_clipped_left_test = clip_gdf(gdf_left_simplified,polygon)


# In[85]:

gdf_clipped_right_test = clip_gdf(gdf_right,polygon)


# In[86]:

get_ipython().magic('matplotlib inline')


# In[89]:

gdf_clipped_left_test["isvalid"] = gdf_clipped_left_test.geometry.is_valid


# In[90]:

gdf_clipped_right_test["isvalid"] = gdf_clipped_right_test.geometry.is_valid


# In[91]:

gdf_union = gpd.overlay(gdf_clipped_left_test,gdf_clipped_right_test,how="union")


# In[ ]:




# In[27]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

gdf_grid_list = np.array_split(gdf_grid, gdf_grid.shape[0])


# In[ ]:

len(gdf_grid_list)


# In[ ]:

gdf_grid_test = gdf_grid[265:266]


# In[ ]:




# In[ ]:




# In[ ]:

p= multiprocessing.Pool(processes=cpu_count)
df_out_list = p.map(create_union_gdfs,gdf_grid_list)
p.close()
p.join()


# In[ ]:

df_out = pd.concat(df_out_list, ignore_index=True)


# In[ ]:

output_path_df = "{}/df_out.pkl".format(ec2_output_path_df)


# In[ ]:

df_out.to_pickle(output_path_df)


# In[ ]:



