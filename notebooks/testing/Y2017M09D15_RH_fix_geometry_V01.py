
# coding: utf-8

# In[25]:

EC2_INPUT_PATH = "/volumes/data/Y2017M09D15_RH_fix_geometry_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M09D15_RH_fix_geometry_V01/output/"

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D29_RH_Merge_FAONames_Upstream_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M09D15_RH_fix_geometry_V01/output/"

INPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_V01.shp"
OUTPUT_FILE_NAME = "hybas_lev06_v1c_merged_fiona_upstream_downstream_FAO_ogr_V01.shp"


# In[26]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')


# In[27]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[28]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[31]:

command = 'ogr2ogr -f "ESRI Shapefile" %s%s %s%s -nlt POLYGON' %(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME,EC2_INPUT_PATH,INPUT_FILE_NAME)


# In[32]:

print(command)


# In[33]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:



