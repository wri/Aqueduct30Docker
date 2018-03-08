
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
# Methodology to apply. 
# 
# 
# if qmax < 1.25 qsum:  
#     q = qmax  
# else:  
#     q = qsum  
#     
# Can be optimized. Options include: Use flow accumulation instead of discharge
# Use multiple level FAmax FAmax-1 FAmax-2 etc. 
# 
# 
# Known issues:  
# When the most downstream pixel is a lake, the blue water available of the lake is available to the entire sub-basin. For example pfaf_id 434210
# 
# Sub-basins which have only one discharge cell of the main river: e.g. 142739 (Famale, Niger)
# 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2018M02D27_RH_Moving_Average_Discharge_EE_V01"

CRS = "EPSG:4326"

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

OUTPUT_VERSION = 2

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160


MA_WINDOW_LENGTH = 10 # Moving average window length. 

TESTING = 0

THRESHOLD = 1.25

PFAF_LEVEL = 6

DIMENSIONS30SSMALL = "43200x19440"
CRS_TRANSFORM30S_SMALL = [0.008333333333333333, 0.0, -180.0, 0.0, -0.008333333333333333, 81.0]


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

geometrySmall = ee.Geometry.Polygon(coords=[[-180.0, -81.0], [180,  -81.0], [180, 81], [-180,81]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[8]:

area30s = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30s_m2V11")
zones30s = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/hybas_lev00_v1c_merged_fiona_30s_V01")
zones30s = zones30s.divide(ee.Number(10).pow(ee.Number(12).subtract(PFAF_LEVEL))).floor().toInt64();

crs30s = area30s.projection()

area30s_pfaf06 = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_30spfaf06_m2_V01V01").select(["sum"])

scale30s = zones30s.projection().nominalScale().getInfo()


# In[9]:

"""
crsTransform5min = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]
"""


# In[10]:

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
    
    i_q_out = use_max.multiply(i_q_max).add((use_sum.multiply(i_q_sum)))
    i_q_out = i_q_out.select(["max"],["b1"]) 
    i_q_out = i_q_out.copyProperties(image)                                                                              
                                                             
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
                                   "reducer",
                                   "description"])
    
    return ee.Image(i_mean)


def mapList(results, key):
    newResult = results.map(lambda x: ee.Dictionary(x).get(key))
    return newResult

def ensure_default_properties(obj): 
    obj = ee.Dictionary(obj)
    default_properties = ee.Dictionary({"mean": -9999,"count": -9999})
    return default_properties.combine(obj)


def zonal_stats_to_raster(image,zonesImage,geometry,maxPixels,reducerType,scale):
    """ Zonal statistics using rasterized zones
    
    Args:
        image (ee.Image) : input image with values (Check the units)
        zonesImage (ee.Image) : integer image with the zones
        geometry (ee.Geometry) : geometry indicating the extent of the calculation. Note if geometry is geodesic
        maxPixels (integer) : maximum numbers of pixels within geometry
        reducerType (string) : options include 'mean', 'max', 'sum', 'first' en 'mode' 
    
    
    
    # reducertype can be mean, max, sum, first. Count is always included for QA
    # the resolution of the zonesimage is used for scale
    """
    
    reducer = ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"mean"),ee.Reducer.mean(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"max"),ee.Reducer.max(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"sum"),ee.Reducer.sum(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"first"),ee.Reducer.first(),
    ee.Algorithms.If(ee.Algorithms.IsEqual(reducerType,"mode"),ee.Reducer.mode(),"error"))))
    )
    reducer = ee.Reducer(reducer).combine(reducer2= ee.Reducer.count(), sharedInputs= True).group(groupField=1, groupName="zones") 

    
    zonesImage = zonesImage.select(zonesImage.bandNames(),["zones"])

    totalImage = ee.Image(image).addBands(zonesImage)
    resultsList = ee.List(totalImage.reduceRegion(
        geometry= geometry, 
        reducer= reducer,
        scale= scale,
        maxPixels=maxPixels,
        bestEffort =True
        ).get("groups"))

    resultsList = resultsList.map(ensure_default_properties); 
    zoneList = mapList(resultsList, 'zones');
    countList = mapList(resultsList, 'count');
    valueList = mapList(resultsList, reducerType);

    valueImage = zonesImage.remap(zoneList, valueList).select(["remapped"],[reducerType])
    countImage = zonesImage.remap(zoneList, countList).select(["remapped"],["count"])
    newImage = zonesImage.addBands(countImage).addBands(valueImage)
    return newImage


def set_properties(image):
    """ Set properties to image based on rows in pandas dataframe
    
    Args:
        image (ee.Image) : image without properties
        
    Returns:
        image_out (ee.Image) : image with properties
    """
    
    properties ={}
    properties["year"] = row["year"]
    properties["month"] = row["month"]
    properties["units"] = "millionm3"
    properties["moving_average_length"] = MA_WINDOW_LENGTH
    properties["moving_average_year_min"] = row["year"]- (MA_WINDOW_LENGTH-1)
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
        description = "{}V{}".format(row["exportdescription"],OUTPUT_VERSION),
        assetId = asset_id,
        dimensions = DIMENSIONS30SSMALL,
        crs = CRS,
        crsTransform = CRS_TRANSFORM30S_SMALL,
        maxPixels = 1e10     
    )
    task.start()
    return asset_id




# In[11]:

area30sPfaf6 = zonal_stats_to_raster(area30s,zones30s,geometrySmall,1e10,"sum",scale30s)


# In[12]:

area30sPfaf6_m2 = area30sPfaf6.select(["sum"]) # image at 30s with area in m^2 per basin


# In[13]:

months = range(1,13)
years = range(1960+9,2014+1)
indicators = ["availabledischarge"]


# In[14]:

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


# In[15]:

df.head()


# In[16]:

if TESTING:
    df = df[0:1]


# In[17]:

df.shape


# In[18]:

for output_ic_assetid in df["output_ic_assetid"].unique():
    result = create_collection(output_ic_assetid)
    print(result)


# In[19]:

function_time_start = datetime.datetime.now()
for index, row in df.iterrows():    
    ic = ee.ImageCollection("{}/global_historical_availableriverdischarge_month_millionm3_5minPfaf6_1960_2014".format(EE_PATH))
    ic_month = ic.filter(ee.Filter.eq("month",row["month"]))
      
    ic_month_simplified = ic_month.map(prepare_discharge_collection)
    i_mean = moving_average_decade(row["year"],ic_month_simplified)
    
    # The result of this operation is at 5arc min. The withdrawal and demand data is at 30s though. Resampling to 30s using the "mode" aka majority
    i_mean_30s = zonal_stats_to_raster(i_mean,zones30s,geometrySmall,1e10,"mode",scale30s).select(["mode"])
    i_mean_30s = i_mean_30s.copyProperties(
        source = i_mean,
        exclude= ["resolution","spatial_resolution"])
    i_mean_30s = set_properties(i_mean_30s)
        
    asset_id = export_asset(i_mean_30s)
    logger.info(asset_id)
    elapsed = datetime.datetime.now() - function_time_start
    print("Processing image {} month {} of year {} runtime {}".format(index,row["month"],row["year"],elapsed))


# In[20]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

