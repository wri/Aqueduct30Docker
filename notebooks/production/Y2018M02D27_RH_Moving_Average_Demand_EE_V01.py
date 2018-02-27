
# coding: utf-8

# # Y2018M02D27_RH_Moving_Average_Demand_EE_V01
# 
# * Purpose of script: Moving average for demand at basin resolution. The script will calculate the volumetric and flux 10 year moving average at a Pfaf6 basin level for total demand. (potentially also per sector demand)
# * Kernel used: python35
# * Date created: 20170227

# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

SCRIPT_NAME = "Y2018M02D27_RH_Moving_Average_Demand_EE_V01"

OUTPUT_VERSION = 1

CRS = "EPSG:4326"

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"


# In[7]:

import ee
import os


# In[9]:

ee.Initialize()


# In[12]:

ic = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PTotWW_month_m_pfaf06_1960_2014")
ic = ic.select("mean")


# The images in the imagecollection have two bands: count and mean. 

# In[ ]:




# In[15]:

def moving_average_decade(year):
    """ Calculate a 10 year moving average
    
    This function is limited to one input paramater to allow mapping over a simple list. 
    Averages the 10 year up to the input year. (]
    
    Global variables required include an imageCollection with a year property.
    
    
    Args:
        year (integer) : final year of interest.
    
    Returns: 
        image (ee.Image) : earth engine image with the mean of the last 10 years
    """
    
    window_length = 10 
    min_year = year - window_length
    
    ic_filtered = (ic.filter(ee.Filter.gt("year",min_year))
                     .filter(ee.Filter.lte("year",year))
                  )
    
    i_mean = ic_filtered.reduce(ee.Reducer.mean()) 
    
    return ee.Image(i_mean)
    


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



