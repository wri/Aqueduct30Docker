
# coding: utf-8

# In[5]:

# Ingests images for hackathon for good


# In[39]:

GCS_INPUT_PATH = "gs://hackathon_for_good_rh_01/RC_Challenge_1/1/georeference_after/"
GCS_PATH = "gs://hackathon_for_good_rh_01/RC_Challenge_1/1/georeference_after_32620/"

SCRIPT_NAME = "hackathon_for_good_upload_images"
OUTPUT_VERSION = 1

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path2 = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)



# In[12]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[13]:

get_ipython().system('gsutil -m cp -r {GCS_INPUT_PATH}/* {ec2_input_path}/.')


# In[20]:

import os 
import subprocess


# In[21]:

files = os.listdir(ec2_input_path)


# In[22]:

files


# In[16]:

## some files are missing crs. Setting to UTM 20 epsg 32620 using gdal


# In[29]:

commands = []
for file in files:    
    source_path = "{}/{}".format(ec2_input_path,file)
    destination_path = "{}/{}".format(ec2_output_path,file)
    
    command = "/opt/anaconda3/envs/python35/bin/gdal_translate -a_srs 'epsg:32620' {} {}".format(source_path,destination_path)
    commands.append(command)   


# In[30]:

for command in commands:
    print(command)
    subprocess.check_output(command,shell=True)


# In[41]:

get_ipython().system('gsutil -m cp -r {ec2_output_path}/* {GCS_PATH}')


# In[40]:

GCS_PATH


# In[42]:

commands = []
for file in files:    
    source_path = "{}/{}".format(GCS_PATH,file)
    file_basename = file[:-4] #remove extension
    file_basename = file_basename.replace(".","")
    metas = file_basename.split("_")
    
    extra_command = ""
    i = 0
    for meta in metas:
        i += 1
        extra_command += " -p meta{}={}".format(i,meta)
    
    
    command = "/opt/anaconda3/envs/python35/bin/earthengine upload image --asset_id='users/rutgerhofste/hackathon_for_good/after/uav_georeferenced/{}' {} --nodata_value=255 {}".format(file_basename,extra_command,source_path) 
    commands.append(command)    


# In[43]:

len(commands)


# In[44]:

commands[1]


# In[45]:

for command in commands:
    print(command)
    subprocess.check_output(command,shell=True)


# In[ ]:

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
    
    
    command2 = "/opt/anaconda3/envs/python35/bin/earthengine upload image --asset_id='users/rutgerhofste/hackathon_for_good/histmat/{}' {} --nodata_value=255 {}".format(file_basename,extra_command,source_path) 
    commands2.append(command2)
    


# In[ ]:

command


# In[ ]:

for command2 in commands2:
    print(command2)
    subprocess.check_output(command2,shell=True)


# In[ ]:



