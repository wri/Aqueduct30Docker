
# coding: utf-8

# In[1]:

""" 
Steps
1. Download datasets
1. Unzip
1. Convert to shapefile
1. Upload to GCS
1. Ingest into ee.


URL: http://www.nationaalgeoregister.nl/geonetwork/srv/dut/catalog.search#/metadata/%7B25943e6e-bb27-4b7a-b240-150ffeaa582e%7D

Todo: 

earthengine currently only supports shapefile. Rewrite for geopackage / geosjon in future. 


"""


SCRIPT_NAME = "Y2018M08D13_RH_Process_Basisregistratie_Gewaspercelen_V01"
OUTPUT_VERSION = 8

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_process_path = "/volumes/data/{}/process_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_process2_path = "/volumes/data/{}/process2_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)



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
import subprocess


# In[4]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('rm -r {ec2_process_path}')
get_ipython().system('rm -r {ec2_process2_path}')
get_ipython().system('rm -r {ec2_input_path}')


# In[5]:

get_ipython().system('mkdir -p {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_process_path}')
get_ipython().system('mkdir -p {ec2_process2_path}')
get_ipython().system('mkdir -p {ec2_input_path}')


# In[6]:

urls = {}
for year in range(2009,2018):
    urls[year] = ("http://geodata.nationaalgeoregister.nl/brpgewaspercelen/extract/{}-definitief/brpgewaspercelen.zip".format(year))

# url for 2018 is slightly different
urls[2018] = ("http://geodata.nationaalgeoregister.nl/brpgewaspercelen/extract/2018-concept/brpgewaspercelen.zip.zip")
    


# In[7]:

for year, url in urls.items():
    output_file_path = ec2_input_path + "BRP{}.zip".format(year)
    command = "wget -O {} '{}'".format(output_file_path,url)
    print(command)
    response = subprocess.check_output(command,shell=True)
    


# In[8]:

files = os.listdir(ec2_input_path)


# In[9]:

files


# In[10]:

for one_file in files:
    command = "unzip -o {}{} -d {}".format(ec2_input_path,one_file,ec2_process_path)
    print(command)
    response = subprocess.check_output(command,shell=True)


# In[11]:

# Inconsistent file name, rename 2014 file

get_ipython().system('mv {ec2_process_path}BRP_Gewaspercelen_definitief.gdb {ec2_process_path}BRP_Gewaspercelen_2014.gdb')


# In[12]:

gdb_files = os.listdir(ec2_process_path)


# In[13]:

for gdb_file in gdb_files:
    if gdb_file.endswith('.gdb'):
        filename, extension = gdb_file.split(".")
        command = "/opt/anaconda3/envs/python35/bin/ogr2ogr -f 'ESRI Shapefile' {}{}.shp {}{}".format(ec2_process2_path,filename,ec2_process_path,gdb_file)
        print(command)
        response = subprocess.check_output(command,shell=True)


# In[14]:

files = os.listdir(ec2_process2_path)


# In[15]:

files


# In[ ]:

for one_file in files:
    if one_file.endswith('.shp'):
        filename, extension = one_file.split(".")
        command = "/opt/anaconda3/envs/python35/bin/ogr2ogr -t_srs EPSG:4326 -s_srs EPSG:28992 {}{} {}{}".format(ec2_output_path,one_file,ec2_process2_path,one_file)
        print(command)
        response = subprocess.check_output(command,shell=True)


# In[ ]:

get_ipython().system('gsutil -m cp {ec2_output_path}* {GCS_OUTPUT_PATH}')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



