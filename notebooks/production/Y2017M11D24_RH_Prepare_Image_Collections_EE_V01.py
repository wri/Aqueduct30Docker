
# coding: utf-8

# # Y2017M11D24_RH_Prepare_Image_Collections_EE_V01
# 
# * Purpose of script: put all earth engine imagecollections in the same format (millionm^3  and dimensionless)
# * Kernel used: python27
# * Date created: 20171124  
# 
# The imageCollections that need a unit conversion are : Discharge and Runoff. 
# 
# 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V07"

AREA_IMAGE_FILE_NAME = "area_5min_m2V11"


SCRIPT_NAME = "Y2017M11D24_RH_Prepare_Image_Collections_EE_V01"

OUTPUT_VERSION = 1

# Unfortunately specifying the dimensions caused the script to crash (internal error on Google's side) Specify scale instead.

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160
CRS = "EPSG:4326"

MAXPIXELS =1e10

icIds = ["projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_riverdischarge_month_m3second_5min_1960_2014",
        "projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_riverdischarge_year_m3second_5min_1960_2014",
        "projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_runoff_month_mmonth_5min_1958_2014",
        "projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_runoff_year_myear_5min_1958_2014"]


# In[3]:

import ee
import re
import subprocess
import pandas as pd
from calendar import monthrange, isleap


# In[4]:

ee.Initialize()


# ICs not in right format: discharge (m^3 / s) and runoff (m/month or m/year)

# In[5]:

sPerD = 86400 #seconds per day


# In[6]:

areaImage = ee.Image("%s/%s"%(EE_PATH,AREA_IMAGE_FILE_NAME))


# In[7]:

dimensions = "%sx%s" %(DIMENSION5MIN["x"],DIMENSION5MIN["y"])


# In[8]:

crsTransform = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[9]:

def newImageId(imageId):
    return re.sub('m3second|mmonth|myear',"millionm3",imageId)

def findUnit(imageId):
    if re.search("m3second",imageId):
        unit = "m3second"
    elif re.search("mmonth",imageId):
        unit = "mmonth"
    elif re.search("myear",imageId):
        unit = "myear"
    else:
        unit = "error"
    return unit


def toVolumeAndExport(row):
    unit = row["unit"]
    temporalResolution = row["image"].get("temporal_resolution").getInfo()
    year = int(row["image"].get("year").getInfo())
    month = int(row["image"].get("month").getInfo())


    
    if unit == "m3second":        
        if temporalResolution == "month":
            sPerMonth = monthrange(year,month)[1]*(86400)
            newImage = row["image"].multiply(sPerMonth).divide(1e6)
            
             
        elif temporalResolution == "year":
            daysPerYear = 365 if isleap(2005) else 366
            sPerYear = daysPerYear*(86400)
            newImage = row["image"].multiply(sPerYear).divide(1e6)
            
        else:
            print("error",row)  
        
    elif unit == "mmonth" or unit == "myear":
        newImage = row["image"].multiply(areaImage).divide(1e6)
    else:
        pass
    
    newImage = newImage.copyProperties(row["image"])
    newImage = newImage.set("units","millionm3")
    newImage = newImage.set("script_used",SCRIPT_NAME)
    newImage = newImage.set("version",OUTPUT_VERSION)
    
    description = "%sV%0.2d" %(row["image"].get("exportdescription").getInfo(),OUTPUT_VERSION)
    print(description)
    
    task = ee.batch.Export.image.toAsset(
        image =  ee.Image(newImage),
        description = description,
        assetId = row["newImageId"],
        dimensions = dimensions,
        crs = CRS,
        crsTransform = crsTransform,
        maxPixels = MAXPIXELS     
    )
    task.start()       
    dfOut.at[index,"newImage"] = newImage
    


# In[10]:

dfIcs = pd.DataFrame()
dfIcs["icId"] = icIds


# ### Creating new ImageCollections

# In[11]:

dfIcs["newIcId"] = dfIcs["icId"].apply(newImageId)


# In[12]:

for index, row in dfIcs.iterrows():
    command = "earthengine create collection %s" %row["newIcId"]
    result = subprocess.check_output(command,shell=True)
    print(command,result)


# In[13]:

dfImages2 = pd.DataFrame()

for index, row in dfIcs.iterrows():
    command = "earthengine ls %s" %row["icId"]
    assetList = subprocess.check_output(command,shell=True).splitlines()
    dfImages = pd.DataFrame(assetList)
    dfImages.columns = ["imageId"]
    dfImages["icId"] = row["icId"]
    dfImages2= dfImages2.append(dfImages)


# In[14]:

dfImages2["newImageId"] = dfImages2["imageId"].apply(newImageId)
dfImages2["newIcId"] = dfImages2["icId"].apply(newImageId)
dfImages2["unit"] = dfImages2["imageId"].apply(findUnit)
dfImages2["image"] = dfImages2["imageId"].apply(lambda x: ee.Image(x))
dfImages2 = dfImages2.set_index("imageId",drop=False)


# In[ ]:

dfOut = dfImages2.copy()


# In[ ]:

i = 0
errorlog = []
startLoop = datetime.datetime.now()
for index, row in dfImages2.iterrows():
    i += 1 
    try:
        toVolumeAndExport(row)
    except:
        errorlog.append(index)
    elapsed = datetime.datetime.now() - startLoop
    print(i,dfImages2.shape[0])
    print(elapsed)
    
    


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

