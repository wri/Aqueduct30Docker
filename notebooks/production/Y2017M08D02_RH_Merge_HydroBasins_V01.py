
# coding: utf-8

# ### Merge WWF's HydroBasins
# 
# * Purpose of script: This notebooks will copy the relevant files from raw ro process and merge the shapefiles of level 6
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170802
# 

# ## Preparation
# 
# make sure you are authorized to use AWS S3
# 
# ## Origin of the WWF data
# 
# The Hydrosheds data has been downloaded from the [WWF Website](http://www.hydrosheds.org/download). A login is required for larger datasets. For Aqueduct we used the Standard version without lakes. Since the download is limited to 5GB we split up the download in two batches:  
# 
# 1. Africa, North American Arctic, Central & South-east Asia, Australia & Oceania, Europe & Middle East
# 1. Greenland, North America & Caribbean, South America, Siberia
# 
# Download URLs (no longer valid)  
# [link1](http://www.hydrosheds.org/tempdownloads/hydrosheds-3926b3742a77b18974ca.zip)  
# [link2](http://www.hydrosheds.org/tempdownloads/hydrosheds-a69872e3f4059aea2434.zip)
# 
# 
# The data was downloaded earlier but replicated here so the latest download data would be 2017/08/03 
# 
# The folders contain all levels but for this phase of Aqueduct we decided to use level 6. More information regarding this decision will be in the methodology document. 
# 
# 
# 

# ## Script
# copy the files from the raw data folder to the process data folder. The raw data folder contains pristine or untouched data and should not be used as a working directory
# 
# 

# In[38]:

get_ipython().system('cd /')


# In[25]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/WWF/HydroSheds30sComplete/HydrobasinsStandardAfr-Eu.zip s3://wri-projects/Aqueduct30/processData/02HydroBasinsV01/')


# In[24]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/rawData/WWF/HydroSheds30sComplete/HydrobasinsStandardGR-SI.zip s3://wri-projects/Aqueduct30/processData/02HydroBasinsV01/')


# In[34]:

get_ipython().system('mkdir /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01')


# In[42]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/processData/02HydroBasinsV01 /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01 --recursive')


# Unzip shapefiles 

# In[45]:

get_ipython().system('cd /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01')


# In[46]:

get_ipython().system('pwd')


# In[48]:

get_ipython().system("find . -name '*.zip' -exec unzip {} \\;")


# For now we will only use Level 6. Extracting in the same directory (/volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01)

# In[53]:

get_ipython().system("find / -name '*lev06_v1c.zip' -exec unzip -o {} \\;")


# In[ ]:




# In[ ]:




# In[61]:

import os
import fiona


# In[54]:

filePath = "/volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01"


# In[56]:

files = os.listdir(filePath)


# In[75]:

get_ipython().system('pwd')


# In[ ]:




# Create output folder

# In[91]:

get_ipython().system('mkdir /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/output')


# In[ ]:

meta = fiona.open('hybas_ar_lev06_v1c.shp').meta
with fiona.open('./output/hybas_lev06_v1c_merged_fiona_V01.shp', 'w', **meta) as output:
    for oneFile in files:    
        if oneFile.endswith(".shp"):
            print(oneFile)
            for features in fiona.open(oneFile):
                output.write(features)
    
    
    


# In[77]:




# In[ ]:




# In[87]:




# In[ ]:




# In[88]:




# In[81]:

get_ipython().system('aws s3 cp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/merge.shp s3://wri-projects/Aqueduct30/temp/ ')
get_ipython().system('aws s3 cp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/merge.dbf s3://wri-projects/Aqueduct30/temp/')
get_ipython().system('aws s3 cp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/merge.prj s3://wri-projects/Aqueduct30/temp/')
get_ipython().system('aws s3 cp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/merge.cpg s3://wri-projects/Aqueduct30/temp/')
get_ipython().system('aws s3 cp /volumes/data/Y2017M08D02_RH_Merge_HydroBasins_V01/merge.shx s3://wri-projects/Aqueduct30/temp/    ')


# In[82]:




# In[83]:




# In[84]:




# In[85]:




# In[ ]:



