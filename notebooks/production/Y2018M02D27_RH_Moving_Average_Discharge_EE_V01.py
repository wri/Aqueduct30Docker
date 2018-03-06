
# coding: utf-8

# # Y2018M02D27_RH_Moving_Average_Discharge_EE_V01
# 
# * Purpose of script: Moving average for discharge at basin resolution. The script will calculate the volumetric and flux 10 year moving average at a Pfaf6 basin level for total demand. (potentially also per sector demand)
# 
# * Script exports to: 
# * Update this projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PTotWW_month_m_pfaf06_1960_2014_movingaverage_10y_V01
# * Update this projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PTotWN_month_m_pfaf06_1960_2014_movingaverage_10y_V01
# * Kernel used: python35
# * Date created: 20170301
# 
# The imageCollection global_historical_availableriverdischarge_month_millionm3_5minPfaf6_1960_2014 (output of script: Y2017M12D07_RH_ZonalStats_MaxQ_toImage_EE_V01) contains three bands: 
# 
# 1. zones_mode_pfaf6  
# 1. sum. sum is the sum of the discharge in millionm3 at the q_search_mask (output of Y2017M12D06_RH_Conservative_Basin_Sinks_EE_V01). q_search_mask is FAmax-1 expect when endorheic or sinks
# 1. max. global maximum of Q within basin. 
# 
# 
# 
# 
# 

# In[ ]:

"""
Methodology to apply. 


if qmax < 1.25 qsum:
    q = qmax
else:
    q = qsum
    
Can be optimized. Options include: Use flow accumulation instead of discharge
Use multiple level FAmax FAmax-1 FAmax-2 etc. 
    

"""


# In[ ]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[ ]:

SCRIPT_NAME = "Y2018M02D27_RH_Moving_Average_Discharge_EE_V01"

CRS = "EPSG:4326"

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

OUTPUT_VERSION = 1

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160


MA_WINDOW_LENGTH = 10 # Moving average window length. 

TESTING = 0


# In[ ]:

import ee
import os
import logging
import pandas as pd
import subprocess


# In[ ]:

ee.Initialize()


# In[ ]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[ ]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[ ]:

crsTransform5minSmall = [
    360.0 / DIMENSION5MIN["x"], 
    0,
    -180,
    0,
    -162.0 / (0.9* DIMENSION5MIN["y"]),
    81   
]

dimensions5minSmall = "{}x{}".format(DIMENSION5MIN["x"],int(0.9*DIMENSION5MIN["y"]))


# In[ ]:

def create_collection(assetid):
    """ Create image collection in earth engine asset folder
    
    This function will only work if the folder in which the
    new imageCollection will be created is valid
    
    
    Args:
        assetid (string) : asset id for the new image collection
    
    Returns: 
        result (string) : captured message from command line
    
    """   
    
    command = "earthengine create collection {}".format(assetid) 
    result = subprocess.check_output(command,shell=True)
    if result:
        logger.error(result)
    return result 


# In[ ]:




# In[ ]:




# In[ ]:

months = range(1,13)
years = range(1960+9,2014+1)
indicators = ["availabledischarge"]


# In[ ]:




# In[ ]:

df = pd.DataFrame()
for indicator in indicators:
    for month in months:
        for year in years:
            newRow = {}
            newRow["month"] = month
            newRow["year"] = year
            newRow["output_ic_filename"] = "global_historical_{}_month_millionm3_5minPfaf06_1960_2014_movingaverage_10y_V{:02.0f}".format(indicator,OUTPUT_VERSION)
            newRow["output_ic_assetid"] = "{}/{}".format(EE_PATH,newRow["output_ic_filename"])
            newRow["output_i_filename"] = "global_historical_{}_month_millionm3_5minPfaf06_Y{:04.0f}M{:02.0f}_movingaverage_10y_V{:02.0f}".format(indicator,year,month,OUTPUT_VERSION)
            newRow["output_i_assetid"] = "{}/{}".format(newRow["output_ic_assetid"],newRow["output_i_filename"])
            newRow["indicator"] = indicator
            newRow["exportdescription"] = "{}_month_Y{:04.0f}M{:02.0f}_movingaverage_10y".format(indicator,year,month)
            df= df.append(newRow,ignore_index=True)


# In[ ]:

df.head()


# In[ ]:

df.shape


# In[ ]:

for output_ic_assetid in df["output_ic_assetid"].unique():
    result = create_collection(output_ic_assetid)
    print(result)


# In[ ]:



