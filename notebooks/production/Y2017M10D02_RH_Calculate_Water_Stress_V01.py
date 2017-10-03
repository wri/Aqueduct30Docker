
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
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M10D02_RH_Calculate_Water_Stress_V01/output/"

EC2_INPUT_PATH = "/volumes/data/Y2017M10D02_RH_Calculate_Water_Stress_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M10D02_RH_Calculate_Water_Stress_V01/output"

INPUT_FILENAME = "Y2017M09D15_RH_Add_Basin_Data_V01"
OUTPUT_FILENAME = "Y2017M10D02_RH_Calculate_Water_Stress_V01"

TEST_BASINS = [292107,292101,292103,292108,292109]


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

dfSelection = dfBasins.loc[TEST_BASINS]


# In[9]:

dfSelection


# In[10]:

columnsOfInterest = ["total_area_30s_m2",
                     "count_area_30s_m2",
                     "Basin_PFAF_IDs",
                     "Upstream_PFAF_IDs",
                     "Downstream_PFAF_IDs",
                     "basin_total_area_30s_m2",
                     "upstream_total_area_30s_m2",
                     "downstream_total_area_30s_m2",
                     "total_volume_PDomWN_yearY2014M12",
                     "upstream_total_volume_PDomWN_yearY2014M12",
                     "downstream_total_volume_PDomWN_yearY2014M12",
                     "basin_total_volume_PDomWN_yearY2014M12",
                     "upstream_total_volume_TotWN_year_Y2014",
                     "upstream_total_volume_reducedmeanrunoff_year_Y1960Y2014",
                     "upstream_total_volume_runoff_yearY2014M12"
                    ]


# In[11]:

dfSimple = dfSelection[columnsOfInterest]


# In[12]:

dfSimple.head()


# In[ ]:




# WS = Local WW / (avail runoff)  = Local WW / (Runoff_up + Runoff_local - WN_up)

# In[ ]:




# In[13]:

demandTypes = ["PDom","PInd","IrrLinear","PLiv"]
useTypes = ["WW","WN"]
temporalResolutions = ["year","month"]
years = [2014]
basinTypes = ["upstream","downstream","basin"]


# In[14]:

demandType = "PDom"
useType = "WW"
temporalResolution = "year"
year = 2014
basinType = "upstream"
month = 12


# In[15]:

dfIn = dfSimple


# In[16]:

dfOut = pd.DataFrame(index=dfIn.index)


# In[17]:

dfOut.head()


# In[18]:

def calculateWaterStressYear(temporalResolution,year,df):
    dfTemp = df.copy()
    dfTemp["ws_yearY%0.4d" %(year)] = dfTemp["total_volume_TotWW_year_Y%0.4d" %(year)] /      (dfTemp["upstream_total_volume_reducedmeanrunoff_year_Y1960Y2014"] +      dfTemp["total_volume_reducedmeanrunoff_year_Y1960Y2014"] -      dfSelection["upstream_total_volume_TotWN_year_Y2014"])
    
def calculateWaterStressMonth(temporalResolution,year,month,df):


# In[19]:

dfOut.head()


# In[25]:

for temporalResolution in temporalResolutions:
    if temporalResolution == "year":
        months = [12]
    elif temporalResolution == "month":
        months = range(1,13)

    for year in years:    
        for month in months:
            for useType in useTypes:
                

    


# In[21]:

dfOut = dfOut.merge(dfSimple,left_index=True,right_index=True,how="left")


# In[22]:

dfOut.head()


# In[23]:

dfOut.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILENAME+"V06.csv"))


# In[24]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:



