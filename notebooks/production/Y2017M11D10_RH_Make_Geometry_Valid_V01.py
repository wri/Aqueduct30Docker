
# coding: utf-8

# ### Merge Upstream Downstream with FAO names 
# 
# * Purpose of script: Create a shapefile and csv file with both the upstream / downstream relation and the FAO basin names
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170829

# This script requires some additional steps that are not automated yet. The objective is to set up a PosGIS enabled PostgreSQL AWS RDS instance. 
# 
# https://gis.stackexchange.com/questions/239198/geopandas-dataframe-to-postgis-table-help
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ConnectToPostgreSQLInstance.html
# 
# http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS
# 
# database is not protected by default. Basic workflow:
# 
# 1. Create database
# 1. Load data into geopandas
# 1. split by geometry type
# 1. upload to postGIS database
# 1. Make valid
# 1. combine results in geopandas

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2017M11D10_RH_Make_Geometry_Valid_V01"

INPUT_VERSION = 1
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

INPUT_FILENAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V%0.2d" %(INPUT_VERSION)

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

OUTPUT_FILE_NAME = "Y2017M11D10_RH_Make_Geometry_Valid_V%0.2d" %(OUTPUT_VERSION)

S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v07"
DATABASE_NAME = "database01"
TABLE_NAME = "hybasvalid01"


# In[3]:

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import *
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
import boto3
import botocore
import time


# Setup of PostGIS Database

# In[4]:

get_ipython().system('aws configure set default.region eu-central-1')


# In[5]:

rds = boto3.client('rds')


# In[6]:

def createDB():
    db_identifier = DATABASE_IDENTIFIER
    rds.create_db_instance(DBInstanceIdentifier=db_identifier,
                       AllocatedStorage=20,
                       DBName=DATABASE_NAME,
                       Engine='postgres',
                       # General purpose SSD
                       StorageType='gp2',
                       StorageEncrypted=False,
                       AutoMinorVersionUpgrade=True,
                       # Set this to true later?
                       MultiAZ=False,
                       MasterUsername='rutgerhofste',
                       MasterUserPassword='nopassword',
                       VpcSecurityGroupIds=['sg-1da15e77'], #You will need to create a security group in the console. 
                       DBInstanceClass='db.t2.micro',
                       Tags=[{'Key': 'test', 'Value': 'test'}])


# In[7]:

createDB()


# In[8]:

# Alternative using Jupyter Magic
#!aws rds create-db-instance --db-name {DATABASE_NAME} --db-instance-identifier {DATABASE_IDENTIFIER} --allocated-storage 20 --db-instance-class "db.t2.micro" --engine "postgres" --master-username "rutgerhofste" --master-user-password "nopassword" --publicly-accessible --vpc-security-group-ids vpc-f6312f9e 


# In[9]:

response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(DATABASE_IDENTIFIER)) 


# In[10]:

status = response["DBInstances"][0]["DBInstanceStatus"]


# In[11]:

# Pause the script while the database is being created
while status != "available":
    response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(DATABASE_IDENTIFIER)) 
    status = response["DBInstances"][0]["DBInstanceStatus"]
    time.sleep(20)
    print(status)


# In[12]:

endpoint = response["DBInstances"][0]["Endpoint"]["Address"]


# In[14]:

print(endpoint)


# Connect to database and setup PostGIS

# In[15]:

engine = create_engine('postgresql://rutgerhofste:nopassword@%s:5432/%s' %(endpoint,DATABASE_NAME))


# In[16]:

connection = engine.connect()


# In[17]:

sqlList = []
sqlList.append("select current_user;")
sqlList.append("create extension postgis;")
sqlList.append("create extension fuzzystrmatch;")
sqlList.append("create extension postgis_tiger_geocoder;")
sqlList.append("create extension postgis_topology;")
sqlList.append("alter schema tiger owner to rds_superuser;")
sqlList.append("alter schema tiger_data owner to rds_superuser;")
sqlList.append("alter schema topology owner to rds_superuser;")
sqlList.append("CREATE FUNCTION exec(text) returns text language plpgsql volatile AS $f$ BEGIN EXECUTE $1; RETURN $1; END; $f$;")      
sqlList.append("SELECT exec('ALTER TABLE ' || quote_ident(s.nspname) || '.' || quote_ident(s.relname) || ' OWNER TO rds_superuser;') FROM ( SELECT nspname, relname FROM pg_class c JOIN pg_namespace n ON (c.relnamespace = n.oid) WHERE nspname in ('tiger','topology') AND relkind IN ('r','S','v') ORDER BY relkind = 'S')s;")
sqlList.append("SET search_path=public,tiger;")
sqlList.append("select na.address, na.streetname, na.streettypeabbrev, na.zip from normalize_address('1 Devonshire Place, Boston, MA 02109') as na;")


# In[18]:

resultList = []
for sql in sqlList:
    #print(sql)
    resultList.append(connection.execute(sql))


# In[19]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[20]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive ')


# In[21]:

get_ipython().magic('matplotlib inline')


# In[22]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".shp"))


# In[23]:

gdf = gdf.set_index("PFAF_ID", drop=False)


# In[24]:

gdf.head()


# In[25]:

gdf.shape


# In[26]:

gdf2 = gdf.copy()


# In[27]:

"""
def explode(indf):
    outdf = gpd.GeoDataFrame(columns=indf.columns)
    for idx, row in indf.iterrows():
        if type(row.geometry) == Polygon:
            outdf = outdf.append(row,ignore_index=True)
        if type(row.geometry) == MultiPolygon:
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row]*recs,ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom,'geometry'] = row.geometry[geom]
            outdf = outdf.append(multdf,ignore_index=True)
    return outdf
"""


# In[28]:

gdf2["type"] = gdf2.geometry.geom_type


# In[29]:

gdfPolygon = gdf2.loc[gdf2["type"]=="Polygon"]
gdfMultiPolygon = gdf2.loc[gdf2["type"]=="MultiPolygon"]


# In[30]:

gdfPolygon2 = gdfPolygon.copy()
gdfMultiPolygon2 = gdfMultiPolygon.copy()


# In[31]:

gdfPolygon2['geom'] = gdfPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[32]:

gdfMultiPolygon2['geom'] = gdfMultiPolygon['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))


# In[33]:

gdfPolygon2.drop("geometry",1, inplace=True)
gdfMultiPolygon2.drop("geometry",1, inplace=True)


# The following command will connect to a temporary free tier AWS RDS instance

# In[34]:

tableNamePolygon = TABLE_NAME+"polygon"
tableNameMultiPolygon = TABLE_NAME+"multipolygon"


# In[35]:

gdfPolygon2.to_sql(tableNamePolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('POLYGON', srid= 4326)})


# In[36]:

gdfMultiPolygon2.to_sql(tableNameMultiPolygon, engine, if_exists='replace', index=False, 
                         dtype={'geom': Geometry('MULTIPOLYGON', srid= 4326)})


# In[37]:

sql = "update %s set geom = st_makevalid(geom)" %(tableNamePolygon)


# In[38]:

result = connection.execute(sql)


# In[39]:

sql = "update %s set geom = st_makevalid(geom)" %(tableNameMultiPolygon)


# In[40]:

result = connection.execute(sql)


# Check if operation succesful 

# In[41]:

sql = "select * from %s" %(tableNamePolygon)


# In[42]:

gdfAWSPolygon=gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' ).set_index("PFAF_ID", drop=False)


# In[43]:

sql = "select * from %s" %(tableNameMultiPolygon)


# In[44]:

gdfAWSMultiPolygon=gpd.GeoDataFrame.from_postgis(sql,connection,geom_col='geom' ).set_index("PFAF_ID", drop=False)


# In[45]:

gdfAWSPolygon.crs = {'init' :'epsg:4326'}
gdfAWSMultiPolygon.crs = {'init' :'epsg:4326'}


# In[46]:

gdfAWS = gdfAWSPolygon.append(gdfAWSMultiPolygon)


# In[47]:

gdfAWS.to_file(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".shp"))


# In[48]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[49]:

connection.close()


# In[50]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

