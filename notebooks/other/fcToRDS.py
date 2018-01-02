
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
# fc to postGIS will just invoke fc -> geopandas 
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
import pandas as pd
import geopandas as gpd


import folium
import folium_gee
import branca 

import shapely
import shapely.wkt

import boto3
import botocore
import sqlalchemy
import geoalchemy2
import geojson
import geojsonio

#from shapely.geometry.multipolygon import MultiPolygon
#from shapely.geometry import shape


# In[3]:

ee.Initialize()


# In[4]:




# In[5]:




# In[6]:




# In[7]:

# Database settings
OUTPUT_VERSION= 1

DATABASE_IDENTIFIER = "aqueduct30v03"
DATABASE_NAME = "database01"
TABLE_NAME = "hydrobasin6_v%0.2d" %(OUTPUT_VERSION)


# In[36]:

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
    """converts a featurecollection to a geoPandas GeoDataFrame. WARNING: Geometries are non-geodesic. Geodesic not yet supported
    
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


def shapelyToEEFeature(row):
    properties = row.drop(["geometry"]).to_dict()
    geoJSONfeature = geojson.Feature(geometry=row["geometry"], properties=properties)
    return ee.Feature(geoJSONfeature)
   

def gdfToFc(gdf):
    """converts a geodataframe  to a featurecollection
    
    Args:
        gdf (geoPandas.GeoDataFrame) : the input geodataframe
        
    Returns:
        fc (ee.FeatureCollection) : feature collection (server side)  
    
    
    """
    gdfCopy = gdf.copy()
    gdfCopy["eeFeature"] = gdfCopy.apply(shapelyToEEFeature,1)
    featureList = gdfCopy["eeFeature"].tolist()
    fc =  ee.FeatureCollection(featureList)
    return fc

def shapelyToFoliumFeature(row):
    """converts a shapely feature to a folium (leaflet) feature. row needs to have a geometry column. CRS is 4326
    
    Args:
        row (geoPandas.GeoDataFrame row) : the input geodataframe row. Appy this function to a geodataframe gdf.appy(function, 1)
        
    Returns:
        foliumFeature  (folium feature) : foliumFeature with popup child.
    
    """    

    width, height = 310,110
    dfTemp = pd.DataFrame(row.drop("geometry"))
    htmlTable = dfTemp.to_html()
    iFrame = branca.element.IFrame(htmlTable, width=width, height=height)
    geoJSONfeature = geojson.Feature(geometry=row["geometry"], properties={})
    foliumFeature = folium.features.GeoJson(geoJSONfeature)
    foliumFeature.add_child(folium.Popup(iFrame))
    return foliumFeature 
    

    
def defaultMap():
    m = folium.Map(
        location=[5, 52],
        tiles='Mapbox Bright',
        zoom_start=4
    )
    return m


def gdfToFoliumGroup(gdf,name="noName",m=None):
    """converts a geodataframe  to a folium featureGroup with the properties as a popup child
    
    Args:
        gdf (geoPandas.GeoDataFrame) : the input geodataframe
        name (string) : output folium feature group name
        
    Returns:
        fc (ee.FeatureCollection) : feature collection (server side)  
    """
     
    featureGroup = folium.FeatureGroup(name=name)
    if m:
        pass
    else:
        m = defaultMap()
    
    features = gdf.apply(shapelyToFoliumFeature,1)   
    map(lambda x: x.add_to(featureGroup),features)
        
    return featureGroup



# # Testing

# tests to perform  
# fc -> Geopandas  
# Geopandas -> postGIS  
# PostGIS -> GeoPandas  
# GeoPandas -> fc  
# 
# Geopandas -> folium  

# In[37]:

fc = ee.FeatureCollection("USDOS/LSIB_SIMPLE/2017");
fcEu = fc.filter(ee.Filter.eq("wld_rgn","Europe"))
fcTest = fcEu.filter(ee.Filter.inList("country_co",["PO","NL"]))


# In[31]:

gdf = fcToGdf(fcTest)


# In[32]:

gdf


# In[40]:

fcTest.getInfo()


# In[41]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[51]:

geometry2 = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=True )


# In[52]:

feature = ee.Feature(geometry,{"rutger":42})
feature2  = ee.Feature(geometry2,{"rutger":42})


# In[55]:

fc1 = ee.FeatureCollection([feature])
fc2 = ee.FeatureCollection([feature2])


# In[56]:

gdf1 = fcToGdf(fc1)
gdf2 = fcToGdf(fc2)


# In[59]:

group1 = gdfToFoliumGroup(gdf1)


# In[60]:

group2 = gdfToFoliumGroup(gdf2)


# In[62]:

m = defaultMap()
group2.add_to(m)
m


# In[ ]:




# In[ ]:




# Plot GDF on folium map with popups

# In[33]:

test = gdfToFoliumGroup(gdf)


# In[35]:

m = defaultMap()
featureGroup.add_to(m)
m

