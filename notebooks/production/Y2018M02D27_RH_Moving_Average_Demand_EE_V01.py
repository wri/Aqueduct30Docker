
# coding: utf-8

# # Y2018M02D27_RH_Moving_Average_Demand_EE_V01
# 
# * Purpose of script: Moving average for demand at basin resolution. The script will calculate the volumetric and flux 10 year moving average at a Pfaf6 basin level for total demand. (potentially also per sector demand)
# 
# * Script exports to: 
# * projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PTotWW_month_m_pfaf06_1960_2014_movingaverage_10y_V01
# * projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PTotWN_month_m_pfaf06_1960_2014_movingaverage_10y_V01
# * Kernel used: python35
# * Date created: 20170227

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2018M02D27_RH_Moving_Average_Demand_EE_V01"

OUTPUT_VERSION = 1

CRS = "EPSG:4326"

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

OUTPUT_VERSION = 1

DIMENSION30S = {}
DIMENSION30S["x"] = 43200
DIMENSION30S["y"] = 21600


MA_WINDOW_LENGTH = 10 # Moving average window length. 


# In[3]:

import ee
import os
import logging
import pandas as pd
import subprocess


# In[4]:

ee.Initialize()


# In[5]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[6]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[7]:

crsTransform30sSmall = [
    360.0 / DIMENSION30S["x"], 
    0,
    -180,
    0,
    -162.0 / (0.9* DIMENSION30S["y"]),
    81   
]


# In[ ]:




# The images in the imagecollection have two bands: count and mean. 

# In[15]:

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
                     .filter(ee.Filter.lte("year",year))
                  )
    
    i_mean = ic_filtered.reduce(ee.Reducer.mean()) 
    
    
    i_mean = i_mean.copyProperties(source=ic_filtered.first(),
                          exclude=["script_used",
                                   "output_version",
                                   "year",
                                   "output_version",
                                   "version",
                                   "reducer"])
    
    return ee.Image(i_mean)


def set_properties(image):
    """ Set properties to image based on rows in pandas dataframe
    
    Args:
        image (ee.Image) : image without properties
        
    Returns:
        image_out (ee.Image) : image with properties
    """
    
    properties ={}
    properties["year"] = row["year"]
    properties["moving_average_length"] = MA_WINDOW_LENGTH
    properties["moving_average_year_min"] = row["year"]- (MA_WINDOW_LENGTH+1)
    properties["script_used"] = SCRIPT_NAME
    properties["indicator"] = row["indicator"]
    properties["version"] = OUTPUT_VERSION
    properties["spatial_resolution"] = "30s"
    properties["exportdescription"] = row["exportdescription"]
    
    image_out = ee.Image(image).set(properties)
    return image_out


def export_asset(image):
    """ Export a google earth engine image to an asset folder
    
    function will start a new task. To view the status of the task
    check the javascript API or query tasks script. Function is used 
    as mapped function so other arguments need to be set globally. 
    
    Args:
        image (ee.Image) : Image to export
        
    Returns:
        asset_id (string) : asset id of     
    """
    
    asset_id = row["output_i_assetid"]
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(image),
        description = row["exportdescription"],
        assetId = asset_id,
        dimensions = DIMENSION30S,
        crs = CRS,
        crsTransform = crsTransform30sSmall,
        maxPixels = 1e10     
    )
    print(asset_id)
    #task.start()



    


# In[9]:

months = range(1,13)
years = range(1960+9,2014+1)
indicators = ["PTotWW","PTotWN"]


# It is only possible to calculate a 10 year running mean starting in 1969

# In[ ]:




# In[10]:

df = pd.DataFrame()

for indicator in indicators:
    for month in months:
        for year in years:
            newRow = {}
            newRow["month"] = month
            newRow["year"] = year
            newRow["output_ic_filename"] = "global_historical_{}_month_m_pfaf06_1960_2014_movingaverage_10y_V{:02.0f}".format(indicator,OUTPUT_VERSION)
            newRow["output_ic_assetid"] = "{}/{}".format(EE_PATH,newRow["output_ic_filename"])
            newRow["output_i_filename"] = "global_historical_{}_month_m_pfaf06_Y{:04.0f}M{:02.0f}_movingaverage_10y_V{:02.0f}".format(indicator,year,month,OUTPUT_VERSION)
            newRow["output_i_assetid"] = "{}/{}".format(newRow["output_ic_assetid"],newRow["output_i_filename"])
            newRow["indicator"] = indicator
            newRow["exportdescription"] = "{}_month_Y{:04.0f}M{:02.0f}_movingaverage_10y".format(indicator,year,month)
            df= df.append(newRow,ignore_index=True)


# In[11]:

df.head()


# In[12]:

for output_ic_assetid in df["output_ic_assetid"].unique():
    result = create_collection(output_ic_assetid)
    print(result)


# In[16]:

function_time_start = datetime.datetime.now()
for index, row in df.iterrows():
    print(index)
    ic = ee.ImageCollection("{}/global_historical_{}_month_m_pfaf06_1960_2014".format(EE_PATH,row["indicator"]))
    ic_month = ic.filter(ee.Filter.eq("month",month))
    i_mean = moving_average_decade(year,ic_month)
    i_mean = set_properties(i_mean)
    export_asset(i_mean)
    elapsed = datetime.datetime.now() - function_time_start
    print("Processing image {} month {} of year {} runtime {}".format(index,month,year,elapsed))


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

