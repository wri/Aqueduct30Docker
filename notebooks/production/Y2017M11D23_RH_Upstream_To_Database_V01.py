
# coding: utf-8

# ### Add upstream PFAF_ID to database
# 
# * Purpose of script: create a table with pfaf_id and upstream_pfaf_id
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171123
# 
# 
# The script requires a file called .password to be stored in the current working directory with the password to the database. Basic functionality
# 

# In[1]:

get_ipython().magic('matplotlib inline')
import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

SCRIPT_NAME = "Y2017M11D23_RH_Upstream_To_Database_V01"

EC2_INPUT_PATH = "/volumes/data/%s/input/" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output/" %(SCRIPT_NAME)

INPUT_VERSION = 1
OUTPUT_VERSION = 1

# Database settings
DATABASE_IDENTIFIER = "aqueduct30v02"
DATABASE_NAME = "database01"

INPUT_FILENAME = ""

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D22_RH_Upstream_V01/output/"


# In[ ]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[ ]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')

