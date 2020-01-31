
# coding: utf-8

# In[15]:

SCRIPT_NAME = "Y2019M07D29_Rh_Temp_Copy_Rasters_V01"
OUTPUT_VERSION = 1

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M07D31_RH_Convert_NetCDF_Geotiff_V02/output_V02"

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


# In[18]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[19]:

indicators = ["dom","ind","irr","liv"]


# In[33]:

import boto3

s3 = boto3.client('s3')
s3.list_objects_v2(Bucket='wri-projects')


# In[ ]:




# In[30]:

for year in range(2000,2015):
    for indicator in indicators:
        print(year, indicator)
        regex = "global_historical_P{}WW_year_millionm3_5min_1960_2014_I***Y{}M12.tif".format(indicator,year)
        input_regex = "{}/{}".format(S3_INPUT_PATH,regex)


# In[31]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {ec2_output_path} --recursive --exclude "*" --include {regex}')


# In[ ]:



