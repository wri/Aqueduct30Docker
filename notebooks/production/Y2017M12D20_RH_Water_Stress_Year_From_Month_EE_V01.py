
# coding: utf-8

# # Y2017M12D20_RH_Water_Stress_Year_From_Month_EE_V01
# 
# * Purpose of script: Calculate annual water stress based on average of all months
# * Kernel used: python27
# * Date created: 20171220

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D20_RH_Water_Stress_Year_From_Month_EE_V01"

OUTPUT_VERSION = 5  #must be same as previous script (stores in same imageCollection)

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

icResults = ee.ImageCollection("%s/reduced_global_historical_combined_V05" %(EE_PATH))


# In[6]:

temporalRange = {}
temporalRange["short"] = [SHORT_TERM_MIN,SHORT_TERM_MAX]
temporalRange["long"] = [LONG_TERM_MIN,LONG_TERM_MAX]


# In[7]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[8]:

def createRow():
    newRow = {}
    newRow["indicator"] = indicator
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
                             "indicator":indicator,
                             "temporalResolution": "year",
                             "month":month,
                             "script_used":SCRIPT_NAME,
                             "units":"dimensionless",
                             "version":OUTPUT_VERSION,
                             "pfaf_level":PFAF_LEVEL,
                             "note": "Indicator is a result of reducing (mean) the 12 monthsto an annual score",
                             "reducer":reducerType}
    newRow["newIcId"]= "%s/reduced_global_historical_combined_V%0.2d" %(EE_PATH,OUTPUT_VERSION)
    newRow["newImageId"] = "%s/global_historical_%s_%s_%s_30sPfaf06_%s_%0.4d_%0.4dM%0.2d" %(newRow["newIcId"],indicator,"year","dimensionless",reducerType,yearMin,yearMax,month)
    newRow["description"] = "reduced_global_historical_combined_%s_%s_%s_V%0.2d" %(interval,reducerType,indicator,OUTPUT_VERSION)
    
    return newRow

def setMinMax(image):
    capped = (image.min(1)).max(0)
    capped = capped.copyProperties(image)
    return capped
    

def reduceTemp(ic):
    ic = ee.ImageCollection(ic)    
    ic = ic.map(setMinMax)    
    reducer = ee.Reducer.mean().combine(reducer2= ee.Reducer.min(), sharedInputs= True ).combine(reducer2= ee.Reducer.max(), sharedInputs= True )
    ic = ic.select(["WS_dimensionless"],["%s_dimensionless" %(indicator)])
    iWSreducedTemp = ee.Image(ic.reduce(reducer))
    return iWSreducedTemp


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
        #task.start()


# In[9]:

reducerTypes = ["mean","trend"]
intervals = ["long","short"]
temporalResolution = "month"


# In[10]:

indicator = "WSreducedTemp"
df = pd.DataFrame()
month = 12

for reducerType in reducerTypes:
    for interval in intervals:
        yearMin = temporalRange[interval][0]
        yearMax = temporalRange[interval][1]
        icTempFiltered = icResults.filter(ee.Filter.eq("temporalResolution",temporalResolution)).filter(ee.Filter.eq("reducer",reducerType)).filter(ee.Filter.eq("interval",interval)).filter(ee.Filter.eq("indicator","WS")).select(["WS_dimensionless"])
        newRow = createRow()
        df = df.append(newRow,ignore_index=True)



# In[11]:

df


# In[12]:

for index, row in df.iterrows():
    print(ee.ImageCollection(row["ic"]).size().getInfo())
    if ee.ImageCollection(row["ic"]).size().getInfo() == 12 :
        iWSreducedTemp = reduceTemp(row["ic"])
        exportAsset(iWSreducedTemp,row["newImageId"],DIMENSIONS30SSMALL,row["description"],row["properties"],CRS_TRANSFORM30S_SMALL)


# In[ ]:




# In[ ]:



