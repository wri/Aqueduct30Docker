
# coding: utf-8

# ### This script allows to get better control over tasks in earth engine
# 
# * Purpose of script: allows the user to list tasks and cancel all pending tasks
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170913

# In[10]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# # Settings

# In[39]:

MAXTASKS = 250
CANCELTASKS = 0


# In[40]:

import pandas as pd
import ee
from retrying import retry
import datetime


# In[41]:

ee.Initialize()


# # Functions

# In[42]:

def get_tasks():
    return ee.batch.Task.list()

def cancel_task(task):
    print task
    random_time = random.random()
    time.sleep(0.5+random_time*0.5)
    if task.config['state'] in (u'RUNNING',u'UNSUBMITTED',u'READY') :
        print 'canceling %s' % task
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
        print i
    return df
    


# In[43]:

taskList = get_tasks()


# In[44]:

detailedTasks = get_details(taskList,MAXTASKS)


# In[45]:

detailedTasks


# # DANGER ZONE

# In[9]:

if CANCELTASKS == 1:
    pendingTasks = [task for task in taskList if task.config['state'] in (u'RUNNING',u'UNSUBMITTED',u'READY')]
    for task in pendingTasks:
        cancel_task(task)

