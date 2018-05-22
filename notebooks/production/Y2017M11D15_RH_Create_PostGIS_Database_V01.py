
# coding: utf-8

# In[1]:

""" Create postgis database using AWS RDS. 
-------------------------------------------------------------------------------

Initiates a postGIS database on Amazon RDS. 

This script requires you to set a password for your database. The script will 
search for the file .password in the current working directory. You can use your
terminal window to create the password. 

Author: Rutger Hofste
Date: 20171115
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04


Args:
    SCRIPT_NAME (string) : Script name.
    DATABASE_IDENTIFIER (string) : Identifier of AWS RDS database.
    DATABASE_NAME (string) : Database name. 






"""


SCRIPT_NAME = "Y2017M11D15_RH_Create_PostGIS_Database_V01"
DATABASE_IDENTIFIER = "aqueduct30v05"
DATABASE_NAME = "database01"



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import boto3
import botocore
from sqlalchemy import *
from geoalchemy2 import Geometry, WKTElement


# In[4]:

rds = boto3.client('rds',region_name="eu-central-1")


# In[5]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()


# In[6]:

def createDB(password):
    db_identifier = DATABASE_IDENTIFIER
    rds.create_db_instance(DBInstanceIdentifier=db_identifier,
                       AllocatedStorage=500,
                       DBName=DATABASE_NAME,
                       Engine='postgres',
                       # General purpose SSD
                       StorageType='gp2',
                       StorageEncrypted=False,
                       AutoMinorVersionUpgrade=True,
                       # Set this to true later?
                       MultiAZ=False,
                       MasterUsername='rutgerhofste',
                       MasterUserPassword=password,
                       VpcSecurityGroupIds=['sg-1da15e77'], #You will need to create a security group in the console. 
                       DBInstanceClass='db.t2.large',
                       Tags=[{'Key': 'author', 'Value': 'rutger'}])


# In[7]:

createDB(password)


# In[8]:

response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(DATABASE_IDENTIFIER))


# In[9]:

status = response["DBInstances"][0]["DBInstanceStatus"]


# In[10]:

# Pause the script while the database is being created
while status != "available":
    response = rds.describe_db_instances(DBInstanceIdentifier="%s"%(DATABASE_IDENTIFIER)) 
    status = response["DBInstances"][0]["DBInstanceStatus"]
    time.sleep(20)
    end = datetime.datetime.now()
    elapsed = end - start
    print(status,elapsed)
    


# In[11]:

endpoint = response["DBInstances"][0]["Endpoint"]["Address"]


# In[12]:

print(endpoint)


# In[13]:

engine = create_engine('postgresql://rutgerhofste:%s@%s:5432/%s' %(password,endpoint,DATABASE_NAME))


# In[14]:

connection = engine.connect()


# [Setting up PostGIS on RDS](http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.PostGIS)

# In[15]:

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


# In[16]:

resultList = []
for sql in sqlList:
    #print(sql)
    resultList.append(connection.execute(sql))


# In[17]:

connection.close()


# In[18]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:06:21.519555

# In[ ]:



