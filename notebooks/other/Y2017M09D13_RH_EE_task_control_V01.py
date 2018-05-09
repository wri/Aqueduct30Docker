
# coding: utf-8

# ### This script allows to get better control over tasks in earth engine
# 
# * Purpose of script: allows the user to list tasks and cancel all pending tasks
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170913

# In[1]:

SCRIPT_NAME = "Y2017M09D13_RH_EE_task_control_V01"
OUTPUT_VERSION = 1
OUTPUT_FILE_NAME = "detailed_tasks"


ec2_output_path = ("/volumes/data/{}/output_V{:02.0f}/").format(SCRIPT_NAME,OUTPUT_VERSION)
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Output s3: " + s3_output_path +
      "\nOutput ec2: " + ec2_output_path)


# In[2]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[3]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# # Settings

# In[4]:

MAXTASKS = 10
CANCELTASKS = 0 # Cancels all pending tasks


# In[5]:

import pandas as pd
import ee
from retrying import retry
import datetime
import random


# In[6]:

ee.Initialize()


# # Functions

# In[7]:

def get_tasks():
    return ee.batch.Task.list()

def cancel_task(task):
    print(task)
    random_time = random.random()
    time.sleep(0.5+random_time*0.5)
    if task.config['state'] in (u'RUNNING',u'UNSUBMITTED',u'READY') :
        print('canceling %s' % task)
        task.cancel()
        
        
@retry(wait_exponential_multiplier=10000, wait_exponential_max=100000)
def checkStatus(task):
    return ee.batch.Task.status(task)
           
def get_details(taskList,MAXTASKS):
    df = pd.DataFrame()
    for i in range(0,min(len(taskList),MAXTASKS)):
        dictNew = checkStatus(taskList[i])
        dfNew = pd.DataFrame(dictNew, index=[i])
        try:
            dfNew["calctime(min)"] = (dfNew["update_timestamp_ms"]-dfNew["start_timestamp_ms"])/(1000*60)
            dfNew["queuetime(min)"] = (dfNew["start_timestamp_ms"]-dfNew["creation_timestamp_ms"])/(1000*60)
            dfNew["runtime(min)"]= dfNew["queuetime(min)"]+dfNew["calctime(min)"]
            dfNew["start_timestamp_UTC"] = datetime.datetime.fromtimestamp(dfNew["start_timestamp_ms"]/1000).strftime('%H:%M:%S')
        except:
            pass
        df = df.append(dfNew)
        print(i)
    return df
    


# In[8]:

taskList = get_tasks()


# In[9]:

type(taskList)


# In[10]:

len(taskList)


# In[11]:

detailedTasks = get_details(taskList,MAXTASKS)


# In[ ]:




# In[12]:

detailedTasks.to_csv(ec2_output_path + OUTPUT_FILE_NAME + ".csv")
detailedTasks.to_pickle(ec2_output_path + OUTPUT_FILE_NAME + ".pkl")


# In[13]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# # DANGER ZONE

# In[14]:

detailedTasks


# In[15]:

if CANCELTASKS == 1:
    pendingTasks = [task for task in taskList if task.config['state'] in (u'RUNNING',u'UNSUBMITTED',u'READY')]
    for task in pendingTasks:
        cancel_task(task)


# In[ ]:



