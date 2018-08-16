
# coding: utf-8

# In[1]:

"""
Ingest shepfile to earthengine


"""

SCRIPT_NAME = 'Y2018M08D13_RH_Ingest_SHP_Earthengine_V01'
OUTPUT_VERSION = 3
OVERWRITE_OUTPUT = 0

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M08D13_RH_Process_Basisregistratie_Gewaspercelen_V01/output_V08/"
EC2_INPUT_PATH = "/volumes/data/Y2018M08D13_RH_Process_Basisregistratie_Gewaspercelen_V01/output_V08"

ee_base = "users/rutgerhofste/{}/".format(SCRIPT_NAME)
ee_output_path = "{}output_V{:02.0f}/".format(ee_base,OUTPUT_VERSION)

print("Input gcs: " + GCS_INPUT_PATH +
      "\nOutput ee: "+ ee_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import subprocess
import aqueduct3
import os


# In[4]:

command = "earthengine create folder users/rutgerhofste/Y2018M08D13_RH_Ingest_SHP_Earthengine_V01"
response = subprocess.check_output(command,shell=True)
command = "earthengine create folder users/rutgerhofste/Y2018M08D13_RH_Ingest_SHP_Earthengine_V01/output_V{:02.0f}".format(OUTPUT_VERSION)
response = subprocess.check_output(command,shell=True)


# In[5]:

files = os.listdir(EC2_INPUT_PATH)


# In[6]:

for one_file in files:
    if one_file.endswith(".shp"):
        filename, extension = one_file.split(".")
        listje = filename.split("_")
        year = listje[2]
        year = int(year)
        input_path = "{}{}".format(GCS_INPUT_PATH,one_file)
        asset_id = "{}{}".format(ee_output_path,filename)
        command = "earthengine upload table  --asset_id={} {} -p year={:04.0f}".format(asset_id, input_path,year)
        print(command)
        response = subprocess.check_output(command,shell=True)


# In[7]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

