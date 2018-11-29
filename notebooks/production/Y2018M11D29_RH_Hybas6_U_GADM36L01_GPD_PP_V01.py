
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


TESTING = 0
SCRIPT_NAME = "Y2018M11D29_RH_Hybas6_U_GADM36L01_GPD_PP_V01"
OUTPUT_VERSION = 5

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_INPUT_TABLE_LEFT = "y2018m11d12_rh_gadm36_level1_to_rds_v01_v02"
RDS_INPUT_TABLE_RIGHT = "hybas06_v04"

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
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


# In[3]:

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


# In[5]:

cpu_count = multiprocessing.cpu_count()
cpu_count = cpu_count -2 #Avoid freeze
print("Power to the maxxx:", cpu_count)


# In[6]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))


# In[7]:

cell_size = 1


# In[8]:

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

def create_union_gdfs(gdf):
    df_out = pd.DataFrame()
    for index, row in gdf.iterrows():
        start = datetime.datetime.now()
        polygon = row.geometry
        destination_path = "{}/gdf_union_{}.pkl".format(ec2_output_path,index)
        try:
            gdf_left_clipped= clip_gdf(gdf_left,polygon)
            gdf_right_clipped = clip_gdf(gdf_right,polygon)
            gdf_union = gpd.overlay(gdf_left_clipped,gdf_right_clipped,how="union",use_sindex=True)
            gdf_union.to_pickle(path=destination_path)
            error = 0
     
        except:
            print("error",destination_path)
            error = 1
        
        end = datetime.datetime.now()
        elapsed = end - start
        outDict = {"error":error,"elapsed":elapsed}
        df_out = pd.DataFrame(outDict,index=[index])
        return df_out
        


# In[9]:

gdf_grid = create_fishnet_gdf(cell_size)


# In[10]:

gdf_grid.head()


# In[11]:

sql = """
SELECT
  gid_1,
  geom
FROM
  {}
""".format(RDS_INPUT_TABLE_LEFT)


# In[12]:

gdf_left = gpd.read_postgis(sql=sql,
                            con=engine)


# In[13]:

gdf_left.head()


# In[14]:

sql = """
SELECT
  pfaf_id,
  geom
FROM
  {}
""".format(RDS_INPUT_TABLE_RIGHT)


# In[15]:

gdf_right = gpd.read_postgis(sql=sql,
                             con=engine)


# In[16]:

gdf_right.head()


# In[17]:

gdf_grid.shape


# In[18]:

#gdf_grid_list = np.array_split(gdf_grid, cpu_count*10)
gdf_grid_list = np.array_split(gdf_grid, gdf_grid.shape[0])


# In[ ]:

p= multiprocessing.Pool()
df_out_list = p.map(create_union_gdfs,gdf_grid_list)
p.close()
p.join()


# In[ ]:

df_out = pd.concat(df_out_list, ignore_index=True)


# In[ ]:

df_out.dtypes


# In[ ]:

df_out["seconds"] = df_out["elapsed"].apply(lambda x: x.total_seconds())


# In[ ]:

output_path_csv = "{}/processing_time.csv".format(ec2_output_path)


# In[ ]:

df_out.to_csv(output_path_csv)


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:45:12.187817 (10x10)
# 0:44:55.686081 (10x10)
#  (1x1 sindex=true)

# In[ ]:



