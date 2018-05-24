
# coding: utf-8

# In[1]:

""" Merge the FAO shapefiles into one Shapefile (UTF-8).
-------------------------------------------------------------------------------

Create a shapefile with the merged files of the FAO database with basin names.

Author: Rutger Hofste
Date: 20170823
Kernel: python27 -> python35
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

SCRIPT_NAME = "Y2017M08D23_RH_Merge_FAONames_V01"
OUTPUT_VERSION  = 2

S3_RAW_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/FAO/namedHydrobasins/"


#S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D23_RH_Merge_FAONames_V01/output/"
#EC2_INPUT_PATH = "/volumes/data/Y2017M08D23_RH_Merge_FAONames_V01/input/"
#EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D23_RH_Merge_FAONames_V01/output/"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_input_path = "s3://wri-projects/Aqueduct30/processData/{}/input_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input ec2: " + ec2_input_path,
      "\nInput s3 raw: " +  S3_RAW_INPUT_PATH,
      "\nInput s3: " + s3_input_path ,
      "\nOutput ec2: " + ec2_output_path,
      "\nOutput s3: " + s3_output_path)



# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# Data URL's, as you can see there is an error with the North America file on FAO's server. The data was downloaded using DownThemAll and stored on S3 

# | Geography | URL |
# |:---|:---|
# |Southeast Asia | http://www.fao.org/geonetwork/srv/en/main.home?uuid=ee616dc4-3118-4d67-ba05-6e93dd3e962f |
# |Near East | http://www.fao.org/geonetwork/srv/en/main.home?uuid=7ae00a40-642b-4637-b1d3-ffacb13360db |
# |Australia & New Zealand | http://www.fao.org/geonetwork/srv/en/main.home?uuid=a1a0e9ee-5062-4950-a6b9-fdd2284b2607 |
# |Africa | http://www.fao.org/geonetwork/srv/en/main.home?uuid=e54e2014-d23b-402b-8e73-c827628d17f4 |
# |Europe | http://www.fao.org/geonetwork/srv/en/main.home?uuid=1849e279-67bd-4e6f-a789-9918925a11a1 |
# |South America | http://www.fao.org/geonetwork/srv/en/main.home?uuid=d47ba28e-31be-470d-81cf-ad3d5594fafd |
# |Central America | http://www.fao.org/geonetwork/srv/en/main.home?uuid=bc9139e6-ccc9-4ded-a0c4-93b91cb54dde |
# |North America | http://ref.data.fao.org/map?entryId=b06dc828-3166-461a-a17d-26f4dc9f9819 |

# In[3]:

get_ipython().system('rm -r {ec2_input_path} ')
get_ipython().system('rm -r {ec2_output_path} ')
get_ipython().system('mkdir -p {ec2_input_path} ')
get_ipython().system('mkdir -p {ec2_output_path} ')


# In[4]:

import os
import fiona


# Copy to working file directory on S3 and EC2

# In[5]:

get_ipython().system('aws s3 cp {S3_RAW_INPUT_PATH} {s3_input_path} --recursive')


# In[6]:

get_ipython().system('aws s3 cp {s3_input_path} {ec2_input_path} --recursive')


# In[7]:

os.chdir(ec2_input_path)
files = os.listdir(ec2_input_path)


# In[8]:

meta = fiona.open('hydrobasins_africa.shp').meta
with fiona.open(ec2_output_path+"/hydrobasins_fao_fiona_merged_v01.shp", 'w', **meta,encoding='UTF-8') as output:
    for oneFile in files:    
        if oneFile.endswith(".shp"):
            print(oneFile)
            for features in fiona.open(oneFile):
                output.write(features)   


# In[10]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive --quiet')


# In[11]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:01:08.475027
# 
# 

# In[ ]:



