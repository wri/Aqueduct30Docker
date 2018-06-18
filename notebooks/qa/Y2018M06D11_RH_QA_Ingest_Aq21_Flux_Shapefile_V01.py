
# coding: utf-8

# In[1]:

""" Ingest aqueduct 2.1 shapefile with fluxes into earthengine.
-------------------------------------------------------------------------------

Input data: 
aqueduct30_v01/Y2018M06D05_RH_QA_Aqueduct21_Flux_Shapefile_V01/output_V03

Author: Rutger Hofste
Date: 20180611
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D11_RH_QA_Ingest_Aq21_Flux_Shapefile_V01'
OUTPUT_VERSION = 3

INPUT_FILE_NAME = "aqueduct21_flux.shp"
OUTPUT_FILE_NAME = "aqueduct21_flux"

EXTRA_PROPERTIES = {"ingested_by" : "RutgerHofste",
                    "script_used": SCRIPT_NAME,
                    "output_version":OUTPUT_VERSION}

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2018M06D05_RH_QA_Aqueduct21_Flux_Shapefile_V01/output_V05"

ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

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


# In[4]:

result = aqueduct3.earthengine.create_ee_folder_recursive(ee_output_path,OVERWRITE_OUTPUT)


# In[5]:

input_path = GCS_INPUT_PATH +"/"+ INPUT_FILE_NAME


# In[6]:

output_path = ee_output_path + "/" + OUTPUT_FILE_NAME


# In[7]:

def property_dict_to_ee_command(d):
    """ Converts a dictionary of properties to earthengine upload command.
    
    Warning: will store all properties as strings.
    
    TODO
    
    Args:
        dictje (Dictionary) : Dictionary with properties
    
    Returns:
        None
    
    """
    
    command = ""
    
    for key, value in d.items():
            command += " --p {}={}".format(key,value)
       
    return command
    
    


# In[8]:

command = "earthengine upload table --asset_id={} {} ".format(output_path,input_path)


# In[9]:

extra_command = property_dict_to_ee_command(EXTRA_PROPERTIES)


# In[10]:

command = command + extra_command


# In[11]:

print(command)


# In[12]:

response = subprocess.check_output(command, shell=True)


# In[13]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:09.885058
# 

# In[ ]:



