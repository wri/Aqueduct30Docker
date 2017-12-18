
# coding: utf-8

# # Y2017M12D15_RH_Statistics_Trend_EE_V01
# 
# * Purpose of script: Calculate short term and long term average and 2014_trend for Q, WW and WN per pfaf 6  basin
# * Kernel used: python27
# * Date created: 20171215

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D15_RH_Statistics_Trend_EE_V01"

OUTPUT_VERSION = 1

PFAF_LEVEL = 6

YEARMIN = 1960
YEARMAX = 2014

SHORT_TERM_MIN = 2004
SHORT_TERM_MAX = 2014

LONG_TERM_MIN = 1960
LONG_TERM_MAX = 2014


DIMENSIONS30SSMALL = "43200x19440"
CRS = "EPSG:4326"
CRS_TRANSFORM30S_SMALL = [0.008333333333333333, 0.0, -180.0, 0.0, -0.008333333333333333, 81.0]


# In[3]:

import ee
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

geometrySmall = ee.Geometry.Polygon(coords=[[-180.0, -81.0], [180,  -81.0], [180, 81], [-180,81]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[7]:

ic = {}
ic["year"] = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_WS_year_dimensionless_30sPfaf06_1960_2014")
ic["month"] = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_WS_month_dimensionless_30sPfaf06_1960_2014")


# In[8]:

temporalRange = {}
temporalRange["short"] = [SHORT_TERM_MIN,SHORT_TERM_MAX]
temporalRange["long"] = [LONG_TERM_MIN,LONG_TERM_MAX]


# Calculate mean 

# In[9]:

def createRow():
    newRow = {}
    newRow["indicatorLong"]= indicator
    newRow["indicator"] = shortIndicator
    newRow["temporalResolution"] = temporalResolution
    newRow["reducerType"] =  reducerType
    newRow["interval"] = interval
    newRow["yearMin"] = yearMin
    newRow["yearMax"] = yearMax 

    newRow["ic"] = icTempFiltered
    newRow["month"] = month
    newRow["properties"] = {"rangeMin":yearMin,
                             "rangeMax":yearMax,
                             "interval":interval,
                             "month":month,
                             "script_used":SCRIPT_NAME,
                             "units":"millionm3",
                             "version":OUTPUT_VERSION,
                             "pfaf_level":PFAF_LEVEL,
                             "reducer":reducerType}
    newRow["newIcId"]= "%s/reduced_global_historical_combined" %(EE_PATH)
    newRow["newImageId"] = "%s/global_historical_%s_%s_%s_30sPfaf06_%s_%0.4d_%0.4dM%0.2d" %(newRow["newIcId"],shortIndicator,temporalResolution,"millionm3",reducerType,yearMin,yearMax,month)
    newRow["description"] = "reduced_global_historical_combined_%s_%s_%s_V%0.2d" %(interval,reducerType,shortIndicator,OUTPUT_VERSION)
    
    return newRow
    


def createCollections(newIcId):        
    command = "earthengine create collection %s" %(newIcId) 
    print(command)
    result = subprocess.check_output(command,shell=True)
    if result:
        pass
    logger.error(result)
    
    
    
def createTimeBand(image):
    # Adds a timeband to the single band image. band is "b1" 
    year = ee.Number(ee.Image(image).get("year"))
    newImage = ee.Image.constant(year).toDouble().select(["constant"],["independent"])
    image = image.toDouble().select([row["indicatorLong"]],["dependent"])
    return image.addBands(newImage)   


def linearTrend(ic,yearmin,yearmax):    
    icTimeband = ee.ImageCollection(ic).map(createTimeBand)  
       
    imageFinalYear = ee.Image(ic.filter(ee.Filter.eq("year",yearmax)))
                          
    fit = icTimeband.select(["independent","dependent"]).reduce(ee.Reducer.linearFit())
    offset = fit.select(["offset"])
    scale = fit.select(["scale"]) #Note that this definition of scale is a as in y = ax+b
    newImageYearMax = scale.multiply(yearmax).add(offset).select(["scale"],["newValue"])
    
    # These lines were added after sharing GDBD results with Sam. Masking out negative values
    PositiveMask = ee.Image(newImageYearMax.gte(0))
    newImageYearMax = ee.Image(newImageYearMax.multiply(PositiveMask))
    
    
    newImageYearMax = newImageYearMax.addBands(offset).addBands(scale)
    return ee.Image(newImageYearMax)

    
def exportAsset(imageOut,assetID,dimensions,description,properties,CRS_TRANSFORM30S_SMALL):
    try:
        ee.Image(assetID).id().getInfo()
        nonExisting = False
    except:
        nonExisting = True
    
    if  nonExisting:  
        imageOut = imageOut.set(properties)

        task = ee.batch.Export.image.toAsset(
            image =  ee.Image(imageOut),
            description = description,
            assetId = assetID,
            dimensions = dimensions,
            crs = CRS,
            crsTransform = CRS_TRANSFORM30S_SMALL,
            maxPixels = 1e10     
        )
        print(assetID)
        logger.debug(assetID)
        task.start()


# In[10]:

temporalResolutions = ["month","year"]


# In[11]:

indicators = ["Q_millionm3","WW_millionm3","WN_millionm3"]


# In[12]:

reducerTypes = ["mean","trend"]


# In[13]:

intervals = ["long","short"]


# In[14]:

df = pd.DataFrame()
for indicator in indicators:
    shortIndicator = indicator[:-10]
    for temporalResolution in temporalResolutions:
        for reducerType in reducerTypes:
            for interval in intervals:
                yearMin = temporalRange[interval][0]
                yearMax = temporalRange[interval][1]
                if temporalResolution == "year":
                    month = 12
                    icTemp = ee.ImageCollection(ic[temporalResolution]).select(indicator)
                    icTempFiltered = icTemp.filter(ee.Filter.gte("year",yearMin)).filter(ee.Filter.lte("year",yearMax))
                    
                    newRow = createRow()
                    df = df.append(newRow,ignore_index=True)                  


                elif temporalResolution == "month":
                    for month in range(1,13):
                        icTemp = ee.ImageCollection(ic[temporalResolution]).select(indicator)
                        icTempFiltered = icTemp.filter(ee.Filter.gte("year",yearMin)).filter(ee.Filter.lte("year",yearMax)).filter(ee.Filter.eq("month",month))
                        newRow = createRow()
                        df = df.append(newRow,ignore_index=True)


# In[15]:

df


# In[16]:

createCollections(df["newIcId"].unique()[0])


# In[17]:

for index, row in df.iterrows():
    if row["reducerType"] == "mean":
        iReduced = ee.Image(ee.ImageCollection(row["ic"]).reduce(ee.Reducer.mean()))
        exportAsset(iReduced,row["newImageId"],DIMENSIONS30SSMALL,row["description"],row["properties"],CRS_TRANSFORM30S_SMALL)
    elif row["reducerType"] == "trend":
        newImageYearMax = linearTrend(ee.ImageCollection(row["ic"]),row["yearMin"],row["yearMax"])
        exportAsset(newImageYearMax,row["newImageId"],DIMENSIONS30SSMALL,row["description"],row["properties"],CRS_TRANSFORM30S_SMALL)

        
        


# In[ ]:



