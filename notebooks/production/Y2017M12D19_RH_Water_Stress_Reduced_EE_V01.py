
# coding: utf-8

# # Y2017M12D19_RH_Water_Stress_Reduced_EE_V01
# 
# * Purpose of script: Calculate water stress using reduced (Long/Short Mean/Trend) maximum discharge and total withdrawals. 
# * Kernel used: python27
# * Date created: 20171219

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

SCRIPT_NAME = "Y2017M12D19_RH_Water_Stress_Reduced_EE_V01"

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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/%sV%0.2d.log" %(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[6]:

icResults = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/reduced_global_historical_combined_V05")


# In[7]:

temporalResolutions = ["month","year"]


# In[8]:

indicators = ["Q_millionm3","WW_millionm3","WN_millionm3"]


# In[9]:

reducerTypes = ["mean","trend"]


# In[10]:

intervals = ["long","short"]


# Water Stress (WS) is defined as total withdrawal (WW) / (Maximum Discharge Q + Local Consumption WN)
# 
# we calculate 4 stress metrics per temporalResolution:
# 
# 1. mean short
# 1. mean long
# 1. trend short
# 1. trend long
# 

# In[11]:

temporalRange = {}
temporalRange["short"] = [SHORT_TERM_MIN,SHORT_TERM_MAX]
temporalRange["long"] = [LONG_TERM_MIN,LONG_TERM_MAX]


# In[12]:

def createRow():
    newRow = {}
    newRow["indicator"] = indicator
    newRow["temporalResolution"] = temporalResolution
    newRow["reducerType"] =  reducerType
    newRow["interval"] = interval
    newRow["yearMin"] = yearMin
    newRow["yearMax"] = yearMax 

    newRow["Q"] = Q
    newRow["WW"] = WW
    newRow["WN"] = WN
    newRow["month"] = month
    newRow["properties"] = {"rangeMin":yearMin,
                             "rangeMax":yearMax,
                             "interval":interval,
                             "indicator":indicator,
                             "temporalResolution": temporalResolution,
                             "month":month,
                             "script_used":SCRIPT_NAME,
                             "units":"dimensionless",
                             "version":OUTPUT_VERSION,
                             "pfaf_level":PFAF_LEVEL,
                             "reducer":reducerType
                           }
    newRow["newIcId"]= "%s/reduced_global_historical_combined_V%0.2d" %(EE_PATH,OUTPUT_VERSION)
    newRow["newImageId"] = "%s/global_historical_%s_%s_%s_30sPfaf06_%s_%0.4d_%0.4dM%0.2d" %(newRow["newIcId"],indicator,temporalResolution,"dimensionless",reducerType,yearMin,yearMax,month)
    newRow["description"] = "reduced_global_historical_combined_%s_%s_%s_V%0.2d" %(interval,reducerType,indicator,OUTPUT_VERSION)
    
    return newRow


def calculateWS(Q,WW,WN):
    BA = Q.add(WN)            
    WS = WW.divide(BA)
    WS = WS.select(WS.bandNames(),["WS_dimensionless"])
    imageOut = WS.addBands(Q).addBands(WW).addBands(WN)
    return imageOut

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


# In[13]:

indicator = "WS"
df = pd.DataFrame()
for temporalResolution in temporalResolutions:
    for reducerType in reducerTypes:
        for interval in intervals:
            yearMin = temporalRange[interval][0]
            yearMax = temporalRange[interval][1]
            if temporalResolution == "year":
                month = 12
                icTemp = icResults.filter(ee.Filter.eq("temporalResolution",temporalResolution)).filter(ee.Filter.eq("reducer",reducerType)).filter(ee.Filter.eq("interval",interval))
                
                Q = ee.Image(icTemp.filter(ee.Filter.eq("indicator","Q")).first())
                WW = ee.Image(icTemp.filter(ee.Filter.eq("indicator","WW")).first())
                WN = ee.Image(icTemp.filter(ee.Filter.eq("indicator","WN")).first())
                
                if reducerType == "trend":
                    Q = Q.select(["newValue"])
                    WW = WW.select(["newValue"])
                    WN = WN.select(["newValue"])
                
                newRow = createRow()
                df = df.append(newRow,ignore_index=True)
            elif temporalResolution == "month":
                for month in range(1,13):
                    icTemp = icResults.filter(ee.Filter.eq("temporalResolution",temporalResolution)).filter(ee.Filter.eq("reducer",reducerType)).filter(ee.Filter.eq("interval",interval)).filter(ee.Filter.eq("month",month))
                    Q = ee.Image(icTemp.filter(ee.Filter.eq("indicator","Q")).first())
                    WW = ee.Image(icTemp.filter(ee.Filter.eq("indicator","WW")).first())
                    WN = ee.Image(icTemp.filter(ee.Filter.eq("indicator","WN")).first())
                    if reducerType == "trend":
                        Q = Q.select(["newValue"])
                        WW = WW.select(["newValue"])
                        WN = WN.select(["newValue"])

                    newRow = createRow()
                    df = df.append(newRow,ignore_index=True)


# In[14]:

df


# In[15]:

for index, row in df.iterrows():
    WS = calculateWS(row["Q"],row["WW"],row["WN"])
    exportAsset(WS,row["newImageId"],DIMENSIONS30SSMALL,row["description"],row["properties"],CRS_TRANSFORM30S_SMALL)


# In[16]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



