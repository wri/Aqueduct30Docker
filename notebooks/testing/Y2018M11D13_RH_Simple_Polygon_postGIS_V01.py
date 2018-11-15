
# coding: utf-8

# In[1]:

# simple polygon to postGIS to test spatial functions


# In[3]:

# Database settings
RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "spatial_test"





# In[4]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement

from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))
connection = engine.connect()


# In[5]:

polys1 = gpd.GeoSeries([Polygon([(0,0), (2,0), (2,2), (0,2)]),
                              Polygon([(2,2), (4,2), (4,4), (2,4)])])


# In[6]:

polys2 = gpd.GeoSeries([Polygon([(1,1), (3,1), (3,3), (1,3)]),
                              Polygon([(3,3), (5,3), (5,5), (3,5)])])


# In[10]:

poly_extent = gpd.GeoSeries([Polygon([(0,0), (10,0), (10,10), (0,10)])])


# In[23]:

poly_extent2 = gpd.GeoSeries([Polygon([(0,-90), (101,-90), (100,0), (0,0)])])


# In[7]:

df1 = gpd.GeoDataFrame({'geometry': polys1, 'df1':[1,2]})


# In[8]:

df2 = gpd.GeoDataFrame({'geometry': polys2, 'df2':[1,2]})


# In[11]:

df_extent = gpd.GeoDataFrame({'geometry': poly_extent, 'id':[1]})


# In[24]:

df_extent2 = gpd.GeoDataFrame({'geometry': poly_extent2, 'id':[1]})


# In[9]:

res_union = gpd.overlay(df1, df2, how='union')


# In[10]:

res_union


# In[11]:

res_symdiff = gpd.overlay(df1, df2, how='symmetric_difference')


# In[12]:

res_symdiff


# In[16]:

def uploadGDFtoPostGIS(gdf,tableName,saveIndex):
    # this function uploads a polygon shapefile to table in AWS RDS. 
    # It handles combined polygon/multipolygon geometry and stores it in valid multipolygon in epsg 4326.
    
    # gdf = input geoDataframe
    # tableName = postGIS table name (string)
    # saveIndex = save index column in separate column in postgresql, otherwise discarded. (Boolean)
    
    
    gdf["type"] = gdf.geometry.geom_type    
    geomTypes = ["Polygon","MultiPolygon"]
    
    for geomType in geomTypes:
        gdfType = gdf.loc[gdf["type"]== geomType]
        geomTypeLower = str.lower(geomType)
        gdfType['geom'] = gdfType['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
        gdfType.drop(["geometry","type"],1, inplace=True)      
        print("Create table temp%s" %(geomTypeLower)) 
        gdfType.to_sql(
            name = "temp%s" %(geomTypeLower),
            con = engine,
            if_exists='replace',
            index= saveIndex, 
            dtype={'geom': Geometry(str.upper(geomType), srid= 4326)}
        )
        
    # Merge both tables and make valid
    sql = []
    sql.append("DROP TABLE IF EXISTS %s"  %(tableName))
    sql.append("ALTER TABLE temppolygon ALTER COLUMN geom type geometry(MultiPolygon, 4326) using ST_Multi(geom);")
    sql.append("CREATE TABLE %s AS (SELECT * FROM temppolygon UNION SELECT * FROM tempmultipolygon);" %(tableName))
    sql.append("UPDATE %s SET geom = st_makevalid(geom);" %(tableName))
    sql.append("DROP TABLE temppolygon,tempmultipolygon")

    for statement in sql:
        print(statement)
        result = connection.execute(statement)    
    gdfFromSQL =gpd.GeoDataFrame.from_postgis("select * from %s" %(tableName),connection,geom_col='geom' )
    return gdfFromSQL


# In[18]:

gdfFromSQL = uploadGDFtoPostGIS(df1,"test.df1",True)


# In[19]:

gdfFromSQL = uploadGDFtoPostGIS(df2,"test.df2",True)


# In[16]:

gdfFromSQL = uploadGDFtoPostGIS(res_symdiff,"test.gpd_symdiff_v01",True)


# In[17]:

gdfFromSQL = uploadGDFtoPostGIS(res_union,"test.gpd_union_v01",True)


# In[17]:

gdfFromSQL = uploadGDFtoPostGIS(df_extent,"test.extent_10degree",True)


# In[26]:

gdfFromSQL = uploadGDFtoPostGIS(df_extent2,"test.extent_big",True)


# In[30]:

sql = """
SELECT
  df1,
  geom,
  ST_AsText(geom) AS wkt
FROM
  {}
""".format("test.df1")


# In[31]:

gdf = gpd.read_postgis(sql=sql,
                       con=engine)


# In[32]:

gdf


# In[35]:

df = pd.DataFrame(gdf.drop("geom",1))


# In[36]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,"df1")


# In[38]:

df.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=100,
          if_exists="replace")


# In[39]:

sql = """
SELECT
  df2,
  geom,
  ST_AsText(geom) AS wkt
FROM
  {}
""".format("test.df2")


# In[40]:

gdf = gpd.read_postgis(sql=sql,
                       con=engine)


# In[41]:

df = pd.DataFrame(gdf.drop("geom",1))


# In[42]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,"df2")


# In[43]:

df.to_gbq(destination_table=destination_table,
          project_id=BQ_PROJECT_ID,
          chunksize=100,
          if_exists="replace")


# In[ ]:



