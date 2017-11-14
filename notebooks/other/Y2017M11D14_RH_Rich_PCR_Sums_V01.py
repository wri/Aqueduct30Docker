
# coding: utf-8

# this script was created per Rich's request. Goal is to calculate total volume of water withdrawal in 2014
# 

# In[16]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[1]:

import ee
import pandas as pd
import os


# In[2]:

ee.Initialize()


# In[3]:

SCRIPT_NAME = "Y2017M11D14_RH_Rich_PCR_Sums_V01"
OUTPUT_VERSION =1 

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

OUTPUT_FILE_NAME = "Y2017M11D14_RH_Rich_PCR_Sums_V%0.2d" %(OUTPUT_VERSION)


# In[4]:

get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[5]:

year = 2014
timeFrame = "year" #annual


# In[6]:

sectors = ["Dom","Ind","Irr","Liv"]


# In[7]:

demandTypes = ["WW","WN"]


# In[8]:

geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[9]:

scale = ee.Image(ee.ImageCollection("projects/WRI-Aquaduct/PCRGlobWB20V07/global_historical_PDomWN_year_millionm3_5min_1960_2014").first()).projection().nominalScale().getInfo()


# In[10]:

print(scale)


# In[11]:

df = pd.DataFrame()
fc = ee.FeatureCollection(ee.Feature(None,{}))
for year in range(1960,2015):
    
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
            
               
    


# In[12]:

df


# In[14]:

df.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUT_FILE_NAME+".csv"))


# In[15]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[17]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



