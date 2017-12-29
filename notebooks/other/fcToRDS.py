
# coding: utf-8

# Goal is to create read / write functions for FeatureCollections to AWS RDS PostGreSQL 
# 
# 
# This script contains the following options:
# 
# fc -> Geopandas -> postGIS  
# PostGIS -> GeoPandas -> fc
# 

# In[1]:

get_ipython().magic('matplotlib inline')


# In[89]:

import ee
import geopandas as gpd
import folium

import shapely

import boto3
import botocore
import sqlalchemy
import geoalchemy2

#from shapely.geometry.multipolygon import MultiPolygon
#from shapely.geometry import shape


# In[3]:

ee.Initialize()


# In[4]:

fc = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017");       


# In[5]:

fcEu = fc.filter(ee.Filter.eq("wld_rgn","Europe"))


# In[76]:

# Database settings
OUTPUT_VERSION= 1

DATABASE_IDENTIFIER = "aqueduct30v03"
DATABASE_NAME = "database01"
TABLE_NAME = "hydrobasin6_v%0.2d" %(OUTPUT_VERSION)


# In[139]:

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
    gdfCopy["geomJSON"] = gdf["geom"].to_json
    
    featureList = []
    
    gdf.apply()
    
    
    geometry = ee.Geometry.Multipolygon([[-121.68, 39.91], [-97.38, 40.34]]);
    properties = {"rutger":42,"freek":26}
    feature = ee.Feature(geometry,properties)
    
    featureList.append(feature)
    
    fc = ee.FeatureCollection(featureList)
    
    return fc


# In[123]:

geom = gdfToFc(gdf)


# In[148]:

gdfCopy = gdf.copy()
gdfCopy["geomJSON"] = gdf["geom"].to_json()


# In[149]:

gdfCopy.head()


# In[150]:

row  = gdfCopy.loc[1]


# In[151]:

geom = row["geomJSON"]


# In[158]:

len(geom)


# In[157]:

ee.Feature(geom,{"rutger":42})


# In[154]:




# In[140]:

gdf2 = gdf.apply(RowAddFeature, axis=1)


# In[138]:

gdf2.head()


# In[109]:

task = ee.batch.Export.table.toDrive(    
    collection =  fcEu ,
    description = "description" ,
    fileNamePrefix = "test01",
    fileFormat = "KML"
)
task.start()


# In[53]:

test = fcEu.getInfo()


# In[65]:

test.keys()


# In[67]:

gdf = fcToGdf(fcEu)


# In[90]:

gdfFromSQL = GdftoPostGIS(connection, gdf,"test01",True)


# In[91]:

gdfFromSQL


# In[54]:

from sys import getsizeof


# In[62]:

features = test["features"]


# In[82]:

engine, connection = rdsConnect(DATABASE_IDENTIFIER,DATABASE_NAME)


# In[83]:

type(engine)


# In[84]:

type(connection)


# In[94]:

gdf = PostGisToGdf(connection,"test01")


# In[97]:




# In[11]:

geoSeries = gpd.GeoSeries(geom2)
geoSeries.crs = {'init' :'epsg:4326'}


# In[12]:

geoSeries.plot()


# In[27]:

gdf = gpd.GeoDataFrame(geometry=geoSeries)


# In[19]:

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))


# In[21]:

world.head()


# In[13]:

geoSeriesJSON = geoSeries.to_json


# In[ ]:




# In[29]:

multiPolygon = folium.features.GeoJson(gdf)


# In[30]:

m = folium.Map([0, 0], zoom_start=3)


# In[31]:

m.add_child(multiPolygon)


# In[ ]:

m


# In[ ]:



