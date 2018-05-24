
# coding: utf-8

# In[1]:

""" Add the FAO Names to the HydroBasins shapefile.
-------------------------------------------------------------------------------
Spatially join FAO Names hydrobasins to the official HydroBasins level 6 
polygons

Author: Rutger Hofste
Date: 20170825
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.    

"""

SCRIPT_NAME = "Y2017M08D25_RH_spatial_join_FAONames_V01"
OUTPUT_VERSION = 7

S3_INPUT_PATH_FAO = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Buffer_FAONames_V01/output_V02/"
S3_INPUT_PATH_HYBAS = "s3://wri-projects/Aqueduct30/processData/Y2017M08D02_RH_Merge_HydroBasins_V02/output_V04/"

INPUT_FILE_NAME_FAO = "hydrobasins_fao_fiona_merged_buffered_v01.shp"
INPUT_FILE_NAME_HYBAS = "hybas_lev06_v1c_merged_fiona_V04.shp"

OUTPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_withFAO_V%0.2d" %(OUTPUT_VERSION)

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ec2: " + ec2_input_path,
      "\nInput s3 FAO: " + S3_INPUT_PATH_FAO,
      "\nInput s3 Hybas: " + S3_INPUT_PATH_HYBAS,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import os
if 'GDAL_DATA' not in os.environ:
    os.environ['GDAL_DATA'] = r'/usr/share/gdal/2.1'
from osgeo import gdal,ogr,osr
'GDAL_DATA' in os.environ
# If false, the GDAL_DATA variable is set incorrectly. You need this variable to obtain the spatial reference
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import time
get_ipython().magic('matplotlib notebook')


# In[4]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_FAO} {ec2_input_path} --recursive ')


# In[6]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_HYBAS} {ec2_input_path} --recursive --exclude *.tif')


# In[7]:

gdfFAO = gpd.read_file(os.path.join(ec2_input_path,INPUT_FILE_NAME_FAO))


# In[8]:

list(gdfFAO)


# In[9]:

gdfHybas = gpd.read_file(os.path.join(ec2_input_path,INPUT_FILE_NAME_HYBAS))


# In[10]:

list(gdfHybas)


# In[11]:

gdfHybas.dtypes


# In[12]:

gdfFAO.dtypes


# In[13]:

gdfFAO['index1_copy'] = gdfFAO['index1']


# In[14]:

gdfFAO = gdfFAO.set_index('index1')


# In[15]:

gdfFAO.index.name


# A spatial join was performed on the data. However the FAO polygons were stored as polygons and not as multi-polygons. The data also lacked a unique Identifier. The identifier consists of a combination of MAJ_BAS and SUB_BASE. The maximum length of MAJ_BAS is 4 and 6 for SUB_BAS (279252). We will store the identifier as a string with the format: MAJ_BASxxxxSUB_BASxxxxxxx (4,7)

# In[16]:

gdfFAO['FAOid'] = gdfFAO.apply(lambda x:'MAJ_BAS_%0.4d_SUB_BAS_%0.7d' % (x['MAJ_BAS'],x['SUB_BAS']),axis=1)


# In[17]:

gdfFAO.index.name


# In[18]:

dfFAO = gdfFAO.drop('geometry',1)


# In[19]:

dfFAO.head()


# In[20]:

gdfFAO['FAOid_copy'] = gdfFAO['FAOid']


# In[21]:

gdfFAO.index.name


# In[22]:

list(gdfFAO)


# In[23]:

gdfFAO = gdfFAO.dissolve(by='FAOid')


# In[24]:

list(gdfFAO)


# In[25]:

dfFAO = gdfFAO.drop('geometry',1)


# In[26]:

dfFAO.head()


# In[27]:

validGeom = gdfFAO.geometry.is_valid


# In[28]:

gdfFAO.crs = {'init': u'epsg:4326'}


# In[29]:

gdfFAO = gdfFAO.set_index('index1_copy')


# In[30]:

gdfJoined = gpd.sjoin(gdfHybas, gdfFAO ,how="left", op='intersects')


# In[31]:

gdfJoined.shape


# In[32]:

series = gdfJoined.groupby('PFAF_ID')['SUB_NAME'].apply(list)
series2 = gdfJoined.groupby('PFAF_ID')['MAJ_NAME'].apply(list)
series3 = gdfJoined.groupby('PFAF_ID')['FAOid_copy'].apply(list)


# In[33]:

df_new1 = series.to_frame()
df_new2 = series2.to_frame()
df_new3 = series3.to_frame()


# In[34]:

df_new1.head()


# In[35]:

df_out = df_new1.merge(right = df_new2, how = "outer", left_index = True, right_index = True )


# In[36]:

df_out = df_out.merge(right = df_new3, how = "outer", left_index = True, right_index = True )


# In[37]:

df_out.dtypes


# In[38]:

df_out.to_csv(os.path.join(ec2_output_path,OUTPUT_FILE_NAME+".csv"),encoding="UTF-8")


# In[39]:

df_out.to_pickle(os.path.join(ec2_output_path,OUTPUT_FILE_NAME+".pkl"))


# ## Linking table

# In[40]:

df_link = gdfJoined[["PFAF_ID","FAOid_copy"]]


# In[41]:

print(os.path.join(ec2_output_path,OUTPUT_FILE_NAME+".csv"))


# In[42]:

df_link.to_csv(os.path.join(ec2_output_path,OUTPUT_FILE_NAME+"_link.csv"),encoding="UTF-8")


# In[43]:

df_link.to_pickle(os.path.join(ec2_output_path,OUTPUT_FILE_NAME+"_link.pkl"))


# In[44]:

df_link.head()


# In[45]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[46]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:06:43.013521

# In[ ]:



