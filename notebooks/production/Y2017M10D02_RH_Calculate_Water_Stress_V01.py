
# coding: utf-8

# # Calculate Water Stress from dataframe
# 
# * Purpose of script: calculate total demand (Dom, IrrLinear, Liv, Ind) and Reduced Runoff and water stress.
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171002

# In[1]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[2]:

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M09D15_RH_Add_Basin_Data_V01/output/"

EC2_INPUT_PATH = "/volumes/data/Y2017M10D02_RH_Calculate_Water_Stress_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M10D02_RH_Calculate_Water_Stress_V01/output"

INPUT_FILENAME = "Y2017M09D15_RH_Add_Basin_Data_V01"

TEST_BASIN = 292107


# Read Pickle file instead of csv 

# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[4]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[6]:

import os
import pandas as pd


# In[7]:

dfBasins = pd.read_pickle(os.path.join(EC2_INPUT_PATH,INPUT_FILENAME+".pkl"))


# In[8]:

test = dfBasins.loc[TEST_BASIN]


# In[9]:

test


# In[10]:

demandTypes = ["PDom","PInd","IrrLinear","PLiv"]
useTypes = ["WW","WN"]
temporalResolutions = ["year","month"]
years = [2014]
basinTypes = ["upstream","downstream","basin"]


# In[29]:

def calculateTotal(basinType,useType,temporalResolution,year,month):
    # This function will add Dom Ind IrrLinear and Livestock of all basins in the input list
    
    if temporalResolution == "year":
        keyTotal = "%s_sum_volumem3_Tot%s_%s_Y%0.4d" %(basinType, useType,temporalResolution,year)
    else:
        keyTotal = "%s_sum_volumem3_Tot%s_%s_Y%0.4dM%0.2d" %(basinType,useType,temporalResolution,year,month)
    
    # Create Column with zeros
    dfDemand[keyTotal] = 0
    for demandType in demandTypes:
        if demandType == "IrrLinear" and temporalResolution == "year":
            # template basin_total_volume_IrrLinearWN_monthY2014M01
            key = "%s_total_volume_%s%s_%sY%0.4d" %(basinType,demandType,useType,temporalResolution,year)
            print(key)
        else:
            key = "%s_total_volume_%s%s_%sY%0.4dM%0.2d" %(basinType,demandType,useType,temporalResolution,year,month)
        dfDemand[keyTotal] = dfDemand[keyTotal] + dfBasins[key]
    return dfDemand   



# In[30]:

demandType = "PDom"
useType = "WW"
temporalResolution = "year"
year = 2014
basinType = "upstream"
month = 12


# In[31]:

dfDemand = pd.DataFrame(index=dfBasins.index)


# In[32]:

dfDemand = calculateTotal(basinType,useType,temporalResolution,year,month)


# In[33]:

dfDemand


# In[11]:

for temporalResolution in temporalResolutions:
    if temporalResolution == "year":
        months = [12]
    elif temporalResolution == "month":
        months = range(1,13)

    for year in years:    
        for month in months:
            for useType in useTypes:
                # Total temporary parameter
                totalTemp = 0
                for demandType in demandTypes:
                    print(demandType,useType,temporalResolution,year,month)

    


# In[ ]:



