
# coding: utf-8

# Goal is to create read / write functions for FeatureCollections to AWS RDS PostGreSQL 
# 
# 
# This script contains the following options:
# 
# fc -> Geopandas -> postGIS  
# PostGIS -> GeoPandas -> fc
# 
# 
# TODO:
# 
# laatste stap: Geopandas - > Fc heeft probelemen met Geometry in GeoJSON
# 
# 
# 

# In[1]:

get_ipython().magic('matplotlib inline')


# In[2]:

import ee
import geopandas as gpd
import folium

import shapely

import boto3
import botocore
import sqlalchemy
import geoalchemy2
import geojson

#from shapely.geometry.multipolygon import MultiPolygon
#from shapely.geometry import shape


# In[7]:

ee.Initialize()


# In[8]:

fc = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017");       


# In[9]:

fcEu = fc.filter(ee.Filter.eq("wld_rgn","Europe"))


# In[10]:

# Database settings
OUTPUT_VERSION= 1

DATABASE_IDENTIFIER = "aqueduct30v03"
DATABASE_NAME = "database01"
TABLE_NAME = "hydrobasin6_v%0.2d" %(OUTPUT_VERSION)


# In[11]:

def rdsConnect(database_identifier,database_name):
    """open a connection to AWS RDS
    
    in addition to specifying the arguments you need to store your password in a file called .password in the current working directory. 
    You can do this using the command line or Jupyter. Make sure to have your .gitignore file up to date.
    
    Args:
        database_identifier (string) : database identifier used when you set up the AWS RDS instance
        database_name (string) : the database name to connect to
        
    Returns:
        engine (sqlalchemy.engine.base.Engine) : database engine
        connection (sqlalchemy.engine.base.Connection) : database connection
    """
    
    
    rds = boto3.client('rds')
    F = open(".password","r")
    password = F.read().splitlines()[0]
    F.close()
    response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(database_identifier))
    status = response["DBInstances"][0]["DBInstanceStatus"]
    print("Status:",status)
    endpoint = response["DBInstances"][0]["Endpoint"]["Address"]
    print("Endpoint:",endpoint)
    engine = sqlalchemy.create_engine('postgresql://rutgerhofste:%s@%s:5432/%s' %(password,endpoint,database_name))
    connection = engine.connect()
    return engine, connection


def fcToGdf(fc, crs = {'init' :'epsg:4326'}):
    """converts a featurecollection to a geoPandas GeoDataFrame
    
    Args:
        fc (ee.FeatureCollection) : the earth engine feature collection to convert. Size is limited to memory (geopandas limitation)
        crs (dictionary, optional) : the coordinate reference system in geopandas format. Defaults to {'init' :'epsg:4326'}
        
    Returns:
        gdf (geoPandas.GeoDataFrame) : the corresponding geodataframe
    
    """
    
    features = fc.getInfo()['features']

    dictarr = []

    for f in features:
        attr = f['properties']
        attr['geometry'] = f['geometry']  
        dictarr.append(attr)

    gdf = gpd.GeoDataFrame(dictarr)
    gdf['geometry'] = map(lambda s: shapely.geometry.shape(s), gdf.geometry)
    gdf.crs = crs
    return gdf


def GdfToPostGIS(connection, gdf,tableName,saveIndex = True):
    """this function uploads a geodataframe to table in AWS RDS.
    
    It handles combined polygon/multipolygon geometry and stores it in valid multipolygon in epsg 4326.
    
    Args:
        connection (sqlalchemy.engine.base.Connection) : postGIS enabled database connection 
        gdf (geoPandas.GeoDataFrame) : input geoDataFrame
        tableName (string) : postGIS table name (string)
        saveIndex (boolean, optional) : save geoDataFrame index column in separate column in postgresql, otherwise discarded. Default is True
        
    Returns:
        gdf (geoPandas.GeoDataFrame) : the geodataframe loaded from the database. Should match the input dataframe
    
    todo:
        currently removes table if exists. Include option to break or append
    
    """   
    
    gdf["type"] = gdf.geometry.geom_type    
    geomTypes = ["Polygon","MultiPolygon"]
    
    for geomType in geomTypes:
        gdfType = gdf.loc[gdf["type"]== geomType]
        geomTypeLower = str.lower(geomType)
        gdfType['geom'] = gdfType['geometry'].apply(lambda x: geoalchemy2.WKTElement(x.wkt, srid=4326))
        gdfType.drop(["geometry","type"],1, inplace=True)      
        print("Create table temp%s" %(geomTypeLower)) 
        gdfType.to_sql(
            name = "temp%s" %(geomTypeLower),
            con = engine,
            if_exists='replace',
            index= saveIndex, 
            dtype={'geom': geoalchemy2.Geometry(str.upper(geomType), srid= 4326)}
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


def PostGisToGdf(connection,tableName):
    """this function gets a geoDataFrame from a postGIS database instance
    
    
    Args:
        connection (sqlalchemy.engine.base.Connection) : postGIS enabled database connection 
        tableName (string) : table name
 
    Returns:
        gdf (geoPandas.GeoDataFrame) : the geodataframe from PostGIS
        
    todo:
        allow for SQL filtering
    
    
    """   
    gdf =gpd.GeoDataFrame.from_postgis("select * from %s" %(tableName),connection,geom_col='geom' )
    gdf.crs =  {'init' :'epsg:4326'}
    return gdf

def RowAddFeature(row):
    """Adds a column with ee features to a geodataframe row
    
    Args:
        gdf row (geoDataFrame row) : the input row
        
    Returns:
        gdf row (geoDataFrame row) : the input row with an added feature
    
    """
    geom = row["geom"]
    geomType = row["geom"].geom_type
    
    if geomType == "MultiPolygon":
        geometry = ee.Geometry.MultiPolygon(geom)
    row["feature"] = geomType
    row["geometry"]  = geometry
    return row
    
    

def gdfToFc(gdf):
    """converts a geodataframe  to a featurecollection
    
    Args:
        gdf (geoPandas.GeoDataFrame) : the input geodataframe
        
    Returns:
        fc (ee.FeatureCollection) : feature collection (server  side)  
    
    
    """
    gdfCopy = gdf.copy()
    gdfCopy["geomJSON"]
    
    featureList = []
    
    
    #geometry = ee.Geometry.Multipolygon([[-121.68, 39.91], [-97.38, 40.34]]);
    #properties = {"rutger":42,"freek":26}
    #feature = ee.Feature(geometry,properties)
    
    #featureList.append(feature)
    
    #fc = ee.FeatureCollection(featureList)
    
    return gdfCopy


# In[12]:

gdf2 = gdf.copy()


# In[ ]:

gdfCopy2 = gdfToFc(gdf)


# In[ ]:

gdfCopy2.head()


# In[ ]:

gdf.shape


# In[ ]:

gdfCopy2["JSON"]  = gdfCopy2["geom"].to_json()


# In[ ]:

gdfCopy2.head()


# In[ ]:

geom = row["JSON"]


# In[ ]:

test = geojson.loads(geom)


# In[ ]:

print(len(test['features']))


# In[ ]:

type(test)


# In[ ]:

test2 = ee.Feature(test,{})


# In[ ]:

geomJSON = geom.to_JSON()


# In[ ]:

len(geom)


# In[ ]:

type(geom)


# In[ ]:

ee.Feature(geom,{"rutger":42})


# In[ ]:




# In[ ]:

gdf2 = gdf.apply(RowAddFeature, axis=1)


# In[ ]:

gdf2.head()


# In[ ]:

task = ee.batch.Export.table.toDrive(    
    collection =  fcEu ,
    description = "description" ,
    fileNamePrefix = "test01",
    fileFormat = "KML"
)
task.start()


# In[ ]:

test = fcEu.getInfo()


# In[ ]:

test.keys()


# In[ ]:

gdf = fcToGdf(fcEu)


# In[ ]:

gdfFromSQL = GdftoPostGIS(connection, gdf,"test01",True)


# In[ ]:

gdfFromSQL


# In[ ]:

from sys import getsizeof


# In[ ]:

features = test["features"]


# In[ ]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[ ]:

type(engine)


# In[ ]:

type(connection)


# In[ ]:

gdf = PostGisToGdf(connection,"test01")


# In[ ]:




# In[ ]:

geoSeries = gpd.GeoSeries(geom2)
geoSeries.crs = {'init' :'epsg:4326'}


# In[ ]:

geoSeries.plot()


# In[ ]:

gdf = gpd.GeoDataFrame(geometry=geoSeries)


# In[ ]:

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))


# In[ ]:

world.head()


# In[ ]:

geoSeriesJSON = geoSeries.to_json


# In[ ]:




# In[ ]:

multiPolygon = folium.features.GeoJson(gdf)


# In[ ]:

m = folium.Map([0, 0], zoom_start=3)


# In[ ]:

m.add_child(multiPolygon)


# In[ ]:

m


# In[ ]:



