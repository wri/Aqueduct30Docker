
# coding: utf-8

# lets try to export GDBD data to shapefile or other Commonly used formats
# 

# In[ ]:




# In[1]:

SCRIPT_NAME = "Y2018M02D13_RH_GDB_To_Shp_V01"


# In[2]:

EC2_INPUT_PATH  = ("/volumes/data/{}/input/").format(SCRIPT_NAME)
EC2_OUTPUT_PATH = ("/volumes/data/{}/output/").format(SCRIPT_NAME)


# In[ ]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[3]:

import subprocess
import pandas as pd
import getpass
from arcgis.gis import *


# In[ ]:




# In[6]:

password = getpass.getpass()


# In[7]:

gis = GIS("https://www.arcgis.com","rhofste_worldresources",password)


# Website http://www.cger.nies.go.jp/db/gdbd/gdbd_index_e.html accessed on Feb 15 2018

# In[9]:

continents = ["Africa","Asia","Europe","Oceania","N_America","S_America"]


# In[10]:

URLs = []

for continent in continents:
    URLs = URLs + ["http://www.cger.nies.go.jp/db/gdbd/data/{}.zip".format(continent)]
    

d = dict(zip(continents, URLs))
df = pd.DataFrame.from_dict(d,orient="index")
df.columns = ['URL']


# In[26]:

df["continent"] = df.index


# In[ ]:

for index, row in df.iterrows():
    command = "wget -O {}{}.zip {}".format(EC2_INPUT_PATH,index,row["URL"])
    print(command)
    subprocess.check_output(command,shell=True)


# Make sure folder is empty

# In[ ]:

for index, row in df.iterrows():
    command = "unzip {}{}.zip -d {}".format(EC2_INPUT_PATH,index,EC2_INPUT_PATH)
    print(command)
    response = subprocess.check_output(command,shell=True)
    print(response)
    


# In[17]:

properties =


# In[30]:

df["properties"] = df.apply(lambda row:  {
                    'title':"GDBD {}".format(row["continent"]),
                    'tags':'GDBD',
                    'type':'Shapefile'}
                            , axis=1) 


# In[32]:

for index, row in df.iterrows():
    print(row["URL"])


# In[ ]:

test_properties = {'title':'Parks and Open Space',
                'tags':'parks, open data, devlabs',
                'type':'Shapefile'}


# In[ ]:




# In[ ]:




# In[ ]:



