
# coding: utf-8

# # Thresholds WaterStress 
# 
# * Purpose of script: add arid and low water use, set thresholds and labels. 
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171004

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# ## Settings

# In[2]:

INPUT_VERSION = 2
OUTPUT_VERSION = 5

SCRIPT_NAME = "Y2017M10D04_RH_Threshold_WaterStress_V02"

S3_INPUT_PATH =  "s3://wri-projects/Aqueduct30/processData/Y2017M10D02_RH_Calculate_Water_Stress_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)


INPUT_FILE_NAME = "Y2017M10D02_RH_Calculate_Water_Stress_V%0.2d" %(INPUT_VERSION)
OUTPUT_FILE_NAME = "Y2017M10D04_RH_Threshold_WaterStress_"

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

YEAR = 2014

THRESHOLD_ARID = 0.03 #units are m/year, threshold defined by Aqueduct 2.1
THRESHOLD_LOW_WATER_USE = 0.012 #units are m/year, threshold defined by Aqueduct 2.1 




# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[4]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[6]:

import pandas as pd
import numpy as np
import os
import math


# In[7]:

df = pd.read_pickle(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME+".pkl"))


# In[8]:

df.head()


# ## Low water use
# 
# Low water use: Water Withdrawal (WW) < 0.012 $m/(m^2*year)$
# 
# total_volume_TotWW_month_Y2014Mxx / total_area_30s_m2 < 0.012 $m/(m^2*year)$
# 
# total_volume_TotWW_year_Y2014 / total_area_30s_m2  < 0.012/12 $m/(m^2*year)$
# 
# ## Arid
# 
# Available Blue Water <0.03 m/(m^2*year)  
# 
# Available blue water = upstream runoff – upstream consumption (WN) + local runoff
# 
# 

# ## Baseline Water Stress Categories
# 
# Baseline water stress raw value to category: $y = max(0,min \big(5,\frac{ln([rawValue])-ln(0.1)}{ln(2)}\big)+1) $

# # Negative Available Blue water
# 
# in the dat from Utrecht University it is possible to have negative local runoff values, leading to a negative available blue water value. These areas are water stressed and should have a categroy 5. This will affect 278 basins that have negative water, 486 basins with 0 water availabel and hence 764 basins in total (<=0)

# In[9]:

def categorizeBWS(rawValue):
    if rawValue ==0:
        catValue= 0
    elif rawValue < 0:
        catValue= -9999
    else: 
        catValue= max(0,min(5,((math.log(rawValue)-math.log(0.1))/(math.log(2)))))
    return catValue


# In[10]:

temporalResolutions = ["month","year"]


# In[11]:

for temporalResolution in temporalResolutions:
    if temporalResolution == "year":
        # Low and Arid
        df["lowWW_year_Y%0.4d" %(YEAR)] = (df['total_volume_TotWW_year_Y2014']/df["total_area_30s_m2"] < THRESHOLD_LOW_WATER_USE).astype(int)
        df["arid_year_Y%0.4d" %(YEAR)] = (df["total_volume_availableSupply_year_Y%0.4d" %(YEAR)] < THRESHOLD_ARID).astype(int)
        df["aridAndLowWW_year_Y%0.4d"%(YEAR)] =   (df["lowWW_year_Y%0.4d" %(YEAR)]&df["arid_year_Y%0.4d" %(YEAR)]).astype(int)
        
        # WS scores
        df['ws_s_excl_aridAndLowWW_year_Y%0.4d'%(YEAR)] = df['ws_year_Y%0.4d'%(YEAR)].apply(categorizeBWS)
        
        # Set Arid and Low WW as Cat 5 
        df['ws_s_year_Y%0.4d' %(YEAR)] = np.where(df["aridAndLowWW_year_Y%0.4d" %(YEAR)],5,df['ws_s_excl_aridAndLowWW_year_Y%0.4d'%(YEAR)])
        
        # Negative Available Supply
        df['ws_s_year_Y%0.4d' %(YEAR)] = np.where(df["total_volume_availableSupply_year_Y%0.4d" %(YEAR)] <= 0 ,5,df['ws_s_year_Y2014'])
        
        
    elif temporalResolution == "month":
        for month in range(1,13):
            df["lowWW_month_Y%0.4dM%0.2d" %(YEAR,month)] = (df["total_volume_TotWW_month_Y%0.4dM%0.2d" %(YEAR,month) ]/df["total_area_30s_m2"] < (THRESHOLD_LOW_WATER_USE/12)).astype(int)
            df["arid_month_Y%0.4dM%0.2d" %(YEAR,month)] = (df["total_volume_availableSupply_month_Y%0.4dM%0.2d" %(YEAR,month)] < (THRESHOLD_ARID/12)).astype(int)
            df["aridAndLowWW_month_Y%0.4dM%0.2d" %(YEAR,month)] = (df["lowWW_month_Y%0.4dM%0.2d" %(YEAR,month)]&df["arid_month_Y%0.4dM%0.2d" %(YEAR,month)]).astype(int)
            df['ws_s_excl_aridAndLowWW_month_Y%0.4dM%0.2d'%(YEAR,month)] = df['ws_month_Y%0.4dM%0.2d' %(YEAR,month)].apply(categorizeBWS)
            df['ws_s_month_Y%0.4dM%0.2d'%(YEAR,month)] = np.where(df["aridAndLowWW_month_Y%0.4dM%0.2d" %(YEAR,month)],5,df['ws_s_excl_aridAndLowWW_month_Y%0.4dM%0.2d'%(YEAR,month)])
            df['ws_s_month_Y%0.4dM%0.2d'%(YEAR,month)] = np.where(df["total_volume_availableSupply_month_Y%0.4dM%0.2d" %(YEAR,month)] <= 0 ,5,df['ws_s_month_Y%0.4dM%0.2d' %(YEAR,month)])                           
                                            
                                            
                                            


# In[12]:

df.head()


# In[13]:

df.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+"V%0.2d.csv" %(OUTPUT_VERSION)))


# In[14]:

df.to_pickle(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+"V%0.2d.pkl" %(OUTPUT_VERSION)))


# In[15]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



