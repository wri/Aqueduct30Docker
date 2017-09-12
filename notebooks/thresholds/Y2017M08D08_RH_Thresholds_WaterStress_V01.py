
# coding: utf-8

# # Thresholds WaterStress 
# 
# * Purpose of script: Double check the threshold setting for the water stress score of Aqueduct 30
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170808
# * Viewed by Tianyi Luo on Sept 12, 2017

# In[1]:

import numpy as np
import pandas as pd
import math
import sys
import boto3
import s3fs


# In[2]:

INPUTPATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Thresholds_WaterStress_V01/input/calculatedWS03.csv"


# In[3]:

OUTPUTPATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Thresholds_WaterStress_V01/output/Y2017M08D08_RH_Thresholds_WaterStress_V01_output.csv"


# In[4]:

TEMP_STORAGE_PATH = '/volumes/data/temp/Y2017M08D08_RH_Thresholds_WaterStress_V01_output.csv'


# In[5]:

sys.version


# In[6]:

df = pd.read_csv(INPUTPATH)


# In[7]:

df.head()


# In[8]:

df.shape


# In[9]:

print(df.columns.values)


# Calculate catchment (local) area in [$m^2$]  
# (You could also just use SUB_AREA but I wanted to double check and this is more precise)

# In[10]:

df['area_m2']= df['meanarea30sm2']*df['countarea30sm2']


# ## Low water use
# 
# Low water use: Water Withdrawal (WW) < 0.012 $m/(m^2*year)$
# 
# local_sum_volumem3_TotWW_yearY2014 < 0.012 $m/(m^2*year)$

# In[11]:

df['local_sum_m_TotWW_yearY2014'] =df['local_sum_volumem3_TotWW_yearY2014']/df['area_m2']


# In[12]:

df['arid'] = df['local_sum_m_TotWW_yearY2014'] < 0.012


# In[13]:

df.arid = df.arid.astype(int)


# ## Arid
# 
# Available Blue Water <0.03 m/(m^2*year)  
# 
# Available blue water = upstream runoff – upstream consumption (WN) + local runoff

# In[14]:

dftemp = pd.DataFrame()


# In[15]:

df['AvailableBlueWaterm3'] = df['upstream_sum_volumem3runoff_annua']- df['upstream_sum_volumem3_TotWN_yearY2014']+ df['local_sum_volumem3_Runoff_yearY2014']


# Convert Volume to flux 

# In[16]:

dftemp['AvailableBlueWaterm'] = df['AvailableBlueWaterm3'] / df['area_m2']


# In[17]:

df['lowWaterUse'] = dftemp['AvailableBlueWaterm'] < 0.03


# In[18]:

df.lowWaterUse = df.lowWaterUse.astype(int)


# ## Arid AND Low water use

# In[19]:

df['aridAndLowWaterUse'] = df['lowWaterUse']&df['arid']


# ## Baseline Water Stress Categories
# 
# Baseline water stress raw value to category: $y = max(0,min \big(5,\frac{ln([rawValue])-ln(0.1)}{ln(2)}\big)+1) $

# In[20]:

def categorizeBWS(rawValue):
    if rawValue ==0:
        catValue= 0
    elif rawValue < 0:
        catValue= -9999
    else: 
        catValue= max(0,min(5,((math.log(rawValue)-math.log(0.1))/(math.log(2)))))
    return catValue


# In[21]:

df['BWS_s_excl_AridAndLow'] = df['ws_yearY2014'].apply(categorizeBWS)


# Arid AND Low Water Use areas are considered category 5

# In[22]:

df['BWS_s'] = df['BWS_s_excl_AridAndLow']


# In[23]:

df['BWS_s'] = np.where(df['aridAndLowWaterUse'],5,df['BWS_s'])


# # Negative Available Blue water
# 
# in the dat from Utrecht University it is possible to have negative local runoff values, leading to a negative available blue water value. These areas are water stressed and should have a categroy 5. This will affect 278 basins that have negative water, 486 basins with 0 water availabel and hence 764 basins in total (<=0)
# 

# In[24]:

df['BWS_s'] = np.where(dftemp['AvailableBlueWaterm'] <= 0 ,5,df['BWS_s'])


# This results in a column with unrounded categorized scores, i.e. 1.2 instead of 2. In order to find the binned score you need to apply a ceiling function. 1.1 -> category 2, 3.2 -> category 4 etc. There is one exception, 0.0 becomes category 1, similar to Aqueduct 2.1

# In[25]:

get_ipython().system('mkdir /volumes/data/temp/')


# In[26]:

df.to_csv(TEMP_STORAGE_PATH)


# In[27]:

get_ipython().system('aws s3 cp {TEMP_STORAGE_PATH} {OUTPUTPATH} --acl public-read')


# In[28]:

df.head()


# In[29]:

df.tail()


# You can find the result on S3 in the location OUTPUTPATH

# In[30]:

print(OUTPUTPATH)


# I made the output public and you should be able to download it using the following [URL](https://s3.amazonaws.com/wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Thresholds_WaterStress_V01/output/Y2017M08D08_RH_Thresholds_WaterStress_V01_output.csv)

# In[ ]:



