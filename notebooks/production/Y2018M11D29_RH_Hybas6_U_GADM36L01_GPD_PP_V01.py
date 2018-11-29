
# coding: utf-8

# In[1]:

""" Union of hydrobasin and GADM 36 level 1 using geopandas parallel processing.
-------------------------------------------------------------------------------

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
Date: 20181128
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""


TESTING = 1
SCRIPT_NAME = "Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_V01"
OUTPUT_VERSION = 1

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_INPUT_TABLE_LEFT = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v02"
RDS_INPUT_TABLE_RIGHT = "hybas06_v04"

ec2_output_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nec2_output_path:", ec2_output_path,
      "\ns3_output_path: ", s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[30]:

import os
import sqlalchemy
import multiprocessing
import numpy as np
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery
from shapely.geometry import MultiPolygon, Polygon


# In[4]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[36]:

cpu_count = multiprocessing.cpu_count()
cpu_count = cpu_count -2 #Avoid freeze
print("Power to the maxxx:", cpu_count)


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[6]:

cell_size = 10


# In[45]:

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

def clip_gdf(gdf,polygon):
    """
    Clip geodataframe using shapely polygon.
    Make sure crs is compatible.
    
    Args:
        gdf (GeoDataFrame): GeoDataFrame in.
        polygon (Shapely Polygon): Polygon used for clipping
    
    """
    crs = gdf.crs
    gdf_intersects = gdf.loc[gdf.geometry.intersects(polygon)]
    df_intersects = gdf_intersects.drop(columns=[gdf_intersects.geometry.name])
    gs_intersections = gpd.GeoSeries(gdf_intersects.geometry.intersection(polygon),crs=crs)
    gdf_clipped = gpd.GeoDataFrame(df_intersects,geometry=gs_intersections)
    return gdf_clipped


# In[8]:

gdf_grid = create_fishnet_gdf(cell_size)


# In[40]:

gdf_grid.head()


# In[9]:

sql = """
SELECT
  gid_1,
  geom
FROM
  {}
""".format(RDS_INPUT_TABLE_LEFT)


# In[10]:

gdf_left = gpd.read_postgis(sql=sql,
                            con=engine)


# In[11]:

gdf_left.head()


# In[12]:

sql = """
SELECT
  pfaf_id,
  geom
FROM
  {}
""".format(RDS_INPUT_TABLE_RIGHT)


# In[13]:

gdf_right = gpd.read_postgis(sql=sql,
                             con=engine)


# In[14]:

gdf_right.head()


# In[46]:

if TESTING:
    gdf_grid = gdf_grid[300:350]


# In[37]:

gdf_grid_list = np.array_split(gdf_grid, cpu_count*100)


# In[43]:

def create_union_gdfs(gdf):
    for index, row in gdf.iterrows():
        polygon = row.geometry
        #gdf_left_clipped= clip_gdf(gdf_left,polygon)
        #gdf_right_clipped = clip_gdf(gdf_right,polygon)
        #gdf_union = gpd.overlay(gdf_left_clipped,gdf_right_clipped,how="union")
        destination_path = "{}/gdf_union_{}.pkl".format(ec2_output_path,index)
        #gdf_union.to_pickle(path=destination_path)
        print()


# In[44]:

p= multiprocessing.Pool()
results_buffered = p.map(create_union_gdfs,gdf_grid_list)
p.close()
p.join()


# In[26]:




# In[28]:




# In[29]:

for 


# In[ ]:




# In[ ]:




# In[ ]:

gdf_grid.to_file(filename=destination_path,
                 driver="GPKG")


# In[ ]:

gdf_union.to_file(filename=destination_path,
                       driver="GPKG")


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:



