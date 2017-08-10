
# coding: utf-8

# # Thresholds Groundwater Stress and Declining Trend
# 
# * Purpose of script: Double check the threshold setting and categorization for the groundwater stress
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170808

# In[1]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/rawData/Deltares/groundwater/2017CRU_EdwinFix/data/tables/aquifer_table_sorted.txt"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Groundwater_Stress_thresholds_V01/output/aquifer_table_sorted.csv"
EC2_INPUT_PATH = "/volumes/data/Y2017M08D08_RH_Groundwater_Stress_thresholds_V01/input/"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M08D08_RH_Groundwater_Stress_thresholds_V01/output/"
NON_SIGNIFICANT_CATEGORY = -9998


# In[2]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')
get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[3]:

import pandas as pd
import s3fs
import os
import numpy as np


# In[4]:

df = pd.read_table(S3_INPUT_PATH,delimiter=";")


# In[5]:

df.head()


# The categorization for groundwater stress is the same as for Aqueduct 2.1
# 
# | category | raw value bins | label |
# |---:|---:|---:|
# |1 | <1 | Low |
# |2 | 1-5 | Low to Medium |
# |3 | 5-10 | Medium to High |
# |4 | 10-20 | High |
# |5 | >20 | Extremely High |
# 
# Areas that are mountaineous should be masked per pixel using a mountain mask. (Sent to Alicia on 2017/07/19)
# 

# In[6]:

def categorizeGS(rawValue):
    if rawValue == -9999:
        catValue= -9999
    elif rawValue < 1 and rawValue >= 0 :
        catValue= 1
    elif rawValue < 5 and rawValue >= 1 :
        catValue= 2
    elif rawValue < 10 and rawValue >= 5 :
        catValue= 3
    elif rawValue < 20 and rawValue >= 10 :
        catValue= 4
    elif rawValue < 9999 and rawValue >= 20 :
        catValue= 5
    else: 
        catValue= -9999
    return catValue

def categorizeGTDT(rawValue):
    if rawValue == -9999:
        catValue= -9999
    elif rawValue < 0 :
        catValue= 1
    elif rawValue < 2 and rawValue >= 0 :
        catValue= 2
    elif rawValue < 8 and rawValue >= 2 :
        catValue= 3
    elif rawValue >= 8 :
        catValue= 4
    else: 
        catValue= -9999
    return catValue


# In[7]:

df['groundwater_stress_categorized'] = df['groundwater_stress'].apply(categorizeGS)


# In[ ]:




# ## Declining Trend
# 
# Categorization for declining trend (based on recommandations by Deltares). Note that Deltares recommend to use four and not five categories. This might have to change int the future to integrate this work into the main Aqueduct workflow. 
# 
# | category | raw value bins | label |
# |---:|---:|---:|
# |1 | <0 cm/yr | No Depletion |
# |2 | 0-2 cm/yr | Low Depletion |
# |3 | 2-8 cm/yr | Moderate Depletion |
# |4 | > 8 | High Depletion |
# 
# Furthermore, we should mask out areas where the trend is not siginicant. (r-squared <0.9) and 
# Aquifers that are not significant will be assiged a value of NON_SIGNIFICANT_CATEGORY = -9998. 

# In[8]:

df['slope_of_decline_cm.year-1_categorized_excl_mask'] = df['slope_of_decline_cm.year-1'].apply(categorizeGTDT)


# In[9]:

df['significant'] =df['r_squared'] > 0.9


# In[10]:

df['slope_of_decline_cm.year-1_categorized'] = np.where(df['significant'],df['slope_of_decline_cm.year-1_categorized_excl_mask'],NON_SIGNIFICANT_CATEGORY)


# In[11]:

df.tail()


# In[12]:

outputLocation = os.path.join(EC2_OUTPUT_PATH,"aquifer_table_sorted.csv")


# In[13]:

print(outputLocation)


# In[14]:

df.to_csv(outputLocation)


# In[15]:

get_ipython().system('aws s3 cp {outputLocation} {S3_OUTPUT_PATH}')


# The file for Aquifer level can be downloaded with this [URL](https://s3.amazonaws.com/wri-projects/Aqueduct30/processData/Y2017M08D08_RH_Groundwater_Stress_thresholds_V01/output/aquifer_table_sorted.csv)

# $$ s_{GW} = \begin{cases} min \left(5,\frac{ln(r_{GW})-ln(5)}{ln(2)}+2 \right) & \mbox{for } r_{GW} \geq 5  \\ 
# 2 & \mbox{for } 3.5 \leq r_{GW} < 5 \\
# max \left(0,\frac{ln(r_{GW}+1.5)-ln(5)}{ln(2)} +2 \right) & \mbox{for } r_{GW} < 3.5
#  \end{cases} $$

# In[ ]:



