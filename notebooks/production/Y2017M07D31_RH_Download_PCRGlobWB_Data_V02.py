
# coding: utf-8

# In[1]:

""" This notebook will download the data from S3 to the EC2 instance 
-------------------------------------------------------------------------------
In this notebook we will copy the data for the first couple of steps from WRI's
Amazon S3 Bucket. The data is large i.e. **40GB** so a good excuse to drink a 
coffee. The output in Jupyter per file is suppressed so you will only see a 
result after the file has been donwloaded. You can also run this command in your
terminal and see the process per file.

The script will rename and copy certain files to create a coherent dataset.

requires AWS cli to be configured.


Author: Rutger Hofste
Date: 20170731
Kernel: python36
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    INPUT_VERSION (integer) : input version, see readme and output number
                              of previous script.
    OUTPUT_VERSION (integer) : output version for ec2 and s3.
    
    
Returns:

Result:
    Unzipped, renamed and restructured files in the EC2 output folder.


"""

# Input Parameters

SCRIPT_NAME = "Y2017M07D31_RH_Download_PCRGlobWB_Data_V02"
PREVIOUS_SCRIPT_NAME = "Y2017M07D31_RH_copy_S3raw_s3process_V02"
INPUT_VERSION = 3
OUTPUT_VERSION = 1

# ETL
s3_input_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(PREVIOUS_SCRIPT_NAME,INPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Input S3:", s3_input_path, "\nOutput ec2:" ,ec2_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

# Imports
import os


# In[4]:

get_ipython().system('rm -r {ec2_output_path}')


# In[5]:

get_ipython().system('mkdir -p {ec2_output_path}')


# In[6]:

get_ipython().system('aws s3 cp {s3_input_path} {ec2_output_path} --recursive')


# In[7]:

#List files downloaded (32 in my case)
get_ipython().system('find {ec2_output_path} -type f | wc -l')


# In[8]:

# As you can see there are some zipped files. Unzipping.  
# Unzipping the file results in a 24GB file which is signifact. Therefore this step will take quite some time

get_ipython().system('unzip {ec2_output_path}totalRunoff_monthTot_output.zip -d {ec2_output_path}')


# The total number of files should be around 25 but can change if the raw data changed. 
# In the data that Yoshi provided there is only Livestock data for consumption (WN). However in an email he specified that the withdrawal (WW) equals the consumption (100% consumption) for livestock. Therefore we copy the WN Livestock files to WW to make looping over WN and WW respectively easier. 

# In[9]:

get_ipython().system('cp {ec2_output_path}/global_historical_PLivWN_month_millionm3_5min_1960_2014.nc4 {ec2_output_path}/global_historical_PLivWW_month_millionm3_5min_1960_2014.nc4')


# In[10]:

get_ipython().system('cp {ec2_output_path}/global_historical_PLivWN_year_millionm3_5min_1960_2014.nc4 {ec2_output_path}/global_historical_PLivWW_year_millionm3_5min_1960_2014.nc4')


# In[11]:

get_ipython().system('ls -lah {ec2_output_path}')


# In[12]:

files = os.listdir(ec2_output_path)
print("Number of files: " + str(len(files)))


# Copy PLivWN to PLivWW because Livestock Withdrawal = Livestock Consumption (see Yoshi's email'). This will solve some lookping issues in the future. Copies 4GB of data so takes a while

# Some files that WRI received from Utrecht refer to water "Use" instead of WN (net). Renaming the relevant file. Renaming them

# In[13]:

get_ipython().system('mv {ec2_output_path}/global_historical_PDomUse_month_millionm3_5min_1960_2014.nc4 {ec2_output_path}/global_historical_PDomWN_month_millionm3_5min_1960_2014.nc4')
get_ipython().system('mv {ec2_output_path}/global_historical_PDomUse_year_millionm3_5min_1960_2014.nc4 {ec2_output_path}/global_historical_PDomWN_year_millionm3_5min_1960_2014.nc4')

get_ipython().system('mv {ec2_output_path}/global_historical_PIndUse_month_millionm3_5min_1960_2014.nc4 {ec2_output_path}/global_historical_PIndWN_month_millionm3_5min_1960_2014.nc4')
get_ipython().system('mv {ec2_output_path}/global_historical_PIndUse_year_millionm3_5min_1960_2014.nc4 {ec2_output_path}/global_historical_PIndWN_year_millionm3_5min_1960_2014.nc4')


# As you can see, the filename structure of the runoff files is different. Using Panoply to inspect the units, we rename the files accordingly. 
# 
# new names for annual:  
# 
# global_historical_runoff_year_myear_5min_1958_2014.nc
# 
# new name for monthly:  
# 
# global_historical_runoff_month_mmonth_5min_1958_2014.nc
# 

# In[14]:

get_ipython().system('mv {ec2_output_path}/totalRunoff_annuaTot_output.nc {ec2_output_path}/global_historical_runoff_year_myear_5min_1958_2014.nc')


# In[15]:

get_ipython().system('mv {ec2_output_path}/totalRunoff_monthTot_output.nc {ec2_output_path}/global_historical_runoff_month_mmonth_5min_1958_2014.nc')


# Final Folder strcuture

# In[16]:

number_of_files =  len(os.listdir(ec2_output_path))


# In[17]:

assert number_of_files == 35, ("Number of files is different than previous run. {} instead of 35".format(number_of_files))


# In[18]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:24:15.930678  
# 0:27:29.926664
# 

# In[ ]:



