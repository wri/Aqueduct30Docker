
# coding: utf-8

# In[1]:

EC2_INPUT_PATH = "/volumes/data/Y2017M09D15_RH_fix_geometry_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M09D15_RH_fix_geometry_V01/output/"

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M09D15_RH_fix_geometry_V01/output/"

INPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V01.shp"
OUTPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_ogr_V01.gml"


# In[2]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')


# In[3]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[ ]:

command = "time ogr2ogr -f "PostgreSQL" PG:"host=localhost user=gis dbname=gis password=gis" wdpa.shp hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_ogr_V01 hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V01 -nln hybas6"


# In[5]:

command = 'ogr2ogr -f "GML" %s%s %s%s -nlt POLYGON' %(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME,EC2_INPUT_PATH,INPUT_FILE_NAME)


# In[6]:

print(command)


# In[8]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:



