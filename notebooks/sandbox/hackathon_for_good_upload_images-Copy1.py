
# coding: utf-8

# In[1]:

# Ingests images for hackathon for good


# In[51]:

GCS_INPUT_PATH = "gs://hackathon_for_good_rh_01/RC_Challenge_1/1/After"
GCS_INPUT_PATH2 = "gs://hackathon_for_good_rh_01/RC_Challenge_1/2"


SCRIPT_NAME = "hackathon_for_good_upload_images"
OUTPUT_VERSION = 1

ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path2 = "/volumes/data/{}/output_V{:02.0f}2".format(SCRIPT_NAME,OUTPUT_VERSION)



# In[9]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[10]:

get_ipython().system('gsutil cp -r {GCS_INPUT_PATH}/* {ec2_output_path}/.')


# In[47]:

import os 
import subprocess


# In[93]:

files = os.listdir(ec2_output_path)


# In[94]:

commands = []
for file in files:
    
    source_path = "{}/{}".format(GCS_INPUT_PATH,file)
    
   
    file_basename = file[:-4] #remove extension
    file_basename = file_basename.replace(".","")
    meta1, meta2, meta3 = file_basename.split("_")
    
    
    command = "/opt/anaconda3/envs/python35/bin/earthengine upload image --asset_id='users/rutgerhofste/hackathon_for_good/after/uav/{}' --nodata_value=255 -p meta1={} -p meta2={} -p meta3={} {}".format(file_basename,meta1,meta2,meta3,source_path) 
    commands.append(command)
    
    


# In[95]:

len(commands)


# In[96]:

for command in commands:
    print(command)
    subprocess.check_output(command,shell=True)


# In[52]:

get_ipython().system('rm -r {ec2_output_path2}')
get_ipython().system('mkdir -p {ec2_output_path2}')


# In[53]:

get_ipython().system('gsutil cp -r {GCS_INPUT_PATH2}/* {ec2_output_path2}/.')


# In[84]:

files2 = os.listdir(ec2_output_path2)


# In[85]:

len(files2)


# In[89]:

commands2 = []
for file2 in files2:    
    source_path = "{}/{}".format(GCS_INPUT_PATH2,file2)
    file_basename = file2[:-4] #remove extension
    file_basename = file_basename.replace(".","")
    metas = file_basename.split("_")
    
    extra_command = ""
    i = 0
    for meta in metas:
        i += 1
        extra_command += " -p meta{}={}".format(i,meta)
    
    
    command2 = "/opt/anaconda3/envs/python35/bin/earthengine upload image --asset_id='users/rutgerhofste/hackathon_for_good/histmat/{}' {} {}".format(file_basename,extra_command,source_path) 
    commands2.append(command2)
    


# In[90]:

command


# In[92]:

for command2 in commands2:
    print(command2)
    subprocess.check_output(command2,shell=True)


# In[ ]:



