
# coding: utf-8

# this script was created per Rich's request. Goal is to calculate total volume of water withdrawal in 2014
# 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

import ee
import pandas as pd
import os


# In[3]:

ee.Initialize()


# In[4]:

SCRIPT_NAME = "Y2017M11D14_RH_Rich_PCR_Sums_V01"
OUTPUT_VERSION =2 

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

OUTPUT_FILE_NAME = "Y2017M11D14_RH_Rich_PCR_Sums_V%0.2d" %(OUTPUT_VERSION)


# In[5]:

get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[6]:

year = 2014
timeFrame = "year" #annual


# In[7]:

sectors = ["Dom","Ind","Irr","Liv"]


# In[8]:

demandTypes = ["WW","WN"]


# In[9]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[10]:

scale = ee.Image(ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PDomWN_year_millionm3_5min_1960_2014").first()).projection().nominalScale().getInfo()


# In[11]:

print(scale)


# In[12]:

df = pd.DataFrame()
fc = ee.FeatureCollection(ee.Feature(None,{}))
for year in range(1960,2014+1):
    
    print(year)
    for sector in sectors:
        for demandType in demandTypes:
            indicator = "%s%s" %(sector,demandType)
            keyName = "%s%sY%0.2d" %(sector,demandType,year)
            filePath = "projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_P%s_year_millionm3_5min_1960_2014" %(indicator)
            ic = ee.ImageCollection(filePath)
            imageYear = ee.Image(ic.filter(ee.Filter.eq("year",year)).first())
            sumYear = imageYear.reduceRegion(
                geometry= geometry,
                reducer= ee.Reducer.sum(),
                scale= scale,
                maxPixels= 1e10
            )
            df.at[year, indicator] = sumYear.get("b1").getInfo()
            
               
    


# ## Add runoff data

# In[13]:

icRunoff = ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_runoff_year_myear_5min_1958_2014")


# In[14]:

imageArea = ee.Image("projects/WRI-Aquaduct/PCRGlobWB20V07/area_5min_m2V11")


# In[15]:

def fluxToVolume(image):
    image = ee.Image(image)
    newImage = image.multiply(imageArea).divide(1e6)
    newImage = newImage.copyProperties(image)
    newImage = newImage.set("units","millionm^3")
    return newImage


# In[16]:

icRunoffMillionm3 = ee.ImageCollection(icRunoff.map(fluxToVolume))


# In[17]:

ic = icRunoffMillionm3
for year in range(1960,2014+1):
    print(year)    
    imageYear = ee.Image(ic.filter(ee.Filter.eq("year",year)).first())
    sumYear = imageYear.reduceRegion(
                geometry= geometry,
                reducer= ee.Reducer.sum(),
                scale= scale,
                maxPixels= 1e10
            )
    df.at[year, "localRunoff"] = sumYear.get("b1").getInfo()
    


# In[18]:

df.head()


# In[19]:

df.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".csv"))


# In[20]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[21]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



