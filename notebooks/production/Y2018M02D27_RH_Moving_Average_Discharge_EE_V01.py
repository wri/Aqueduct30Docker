
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

# In[1]:

"""
Methodology to apply. 


if qmax < 1.25 qsum:
    q = qmax
else:
    q = qsum
    
Can be optimized. Options include: Use flow accumulation instead of discharge
Use multiple level FAmax FAmax-1 FAmax-2 etc. 
    

"""


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

SCRIPT_NAME = "Y2018M02D27_RH_Moving_Average_Discharge_EE_V01"

CRS = "EPSG:4326"

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

OUTPUT_VERSION = 1

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160


MA_WINDOW_LENGTH = 10 # Moving average window length. 

TESTING = 1

THRESHOLD = 1.25


# In[4]:

import ee
import os
import logging
import pandas as pd
import subprocess


# In[5]:

ee.Initialize()


# In[6]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[7]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[8]:

crsTransform5minSmall = [
    360.0 / DIMENSION5MIN["x"], 
    0,
    -180,
    0,
    -162.0 / (0.9* DIMENSION5MIN["y"]),
    81   
]

dimensions5minSmall = "{}x{}".format(DIMENSION5MIN["x"],int(0.9*DIMENSION5MIN["y"]))


# In[9]:

def prepare_discharge_collection(image):
    """ find the available discharge based on max and sum bands of available discharge
    
    if qmax =< threshold (1.25) qsum:
        q = qmax
    else:
        q = qsum
    
    Args:
        i_in (ee.Image) :image of available discharge with three bands: zones, max and sum
    
    Returns:
        i_q_out (ee.Image) : image with only one band 'b1'
    
    
    """
    
    i_q_max = image.select(["max"])  
    i_q_sum = image.select(["sum"])
    
    i_ratio_q = i_q_max.divide(i_q_sum)
       
    use_max = i_ratio_q.lte(THRESHOLD)
    use_sum = i_ratio_q.gt(THRESHOLD)
    
    i_q_out = use_max.multiply(i_q_max).add((use_sum.multiply(i_q_sum)) 
    i_q_out = i_q_out.select(["max"],["b1"])                                         
    
    return i_q_out
                                  
                                  

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


def moving_average_decade(year,ic):
    """ Calculate a 10 year moving average
    
    This function is limited to one input paramater to allow mapping over a simple list. 
    Averages the 10 year up to the input year. (]
    
    Global variables required include an imageCollection with a year property.
    
    
    Args:
        year (integer) : final year of interest.
        ic (ee.ImageCollection) : input imageCollection
    
    Returns: 
        image (ee.Image) : earth engine image with the mean of the last 10 years
    """
    
    min_year = year - MA_WINDOW_LENGTH
    
    ic_filtered = (ic.filter(ee.Filter.gt("year",min_year))
                     .filter(ee.Filter.lte("year",year)))
                  
    i_mean = ic_filtered.reduce(ee.Reducer.mean()) 
    
    
    i_mean = i_mean.copyProperties(source=ic_filtered.first(),
                          exclude=["script_used",
                                   "output_version",
                                   "year",
                                   "output_version",
                                   "version",
                                   "reducer"])
    
    return ee.Image(i_mean)


# In[ ]:




# In[ ]:




# In[10]:

months = range(1,13)
years = range(1960+9,2014+1)
indicators = ["availabledischarge"]


# In[ ]:




# In[11]:

df = pd.DataFrame()
for indicator in indicators:
    for month in months:
        for year in years:
            newRow = {}
            newRow["month"] = month
            newRow["year"] = year
            newRow["output_ic_filename"] = "global_historical_{}_month_millionm3_pfaf06_1960_2014_movingaverage_10y_V{:02.0f}".format(indicator,OUTPUT_VERSION)
            newRow["output_ic_assetid"] = "{}/{}".format(EE_PATH,newRow["output_ic_filename"])
            newRow["output_i_filename"] = "global_historical_{}_month_millionm3_pfaf06_Y{:04.0f}M{:02.0f}_movingaverage_10y_V{:02.0f}".format(indicator,year,month,OUTPUT_VERSION)
            newRow["output_i_assetid"] = "{}/{}".format(newRow["output_ic_assetid"],newRow["output_i_filename"])
            newRow["indicator"] = indicator
            newRow["exportdescription"] = "{}_month_Y{:04.0f}M{:02.0f}_movingaverage_10y".format(indicator,year,month)
            df= df.append(newRow,ignore_index=True)


# In[12]:

df.head()


# In[13]:

if TESTING:
    df = df[0:1]


# In[14]:

df.shape


# In[15]:

for output_ic_assetid in df["output_ic_assetid"].unique():
    result = create_collection(output_ic_assetid)
    print(result)


# In[ ]:




# In[ ]:




# In[18]:

function_time_start = datetime.datetime.now()
for index, row in df.iterrows():    
    ic = ee.ImageCollection("{}/global_historical_availableriverdischarge_month_millionm3_5minPfaf6_1960_2014".format(EE_PATH))
    ic_month = ic.filter(ee.Filter.eq("month",row["month"]))
    i_mean = moving_average_decade(row["year"],ic_month)


# In[19]:

print(i_mean.getInfo())


# In[ ]:



