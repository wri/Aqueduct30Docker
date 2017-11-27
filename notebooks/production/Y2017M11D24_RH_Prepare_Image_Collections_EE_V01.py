
# coding: utf-8

# # Y2017M11D24_RH_Prepare_Image_Collections_EE_V01
# 
# * Purpose of script: put all earth engine imagecollections in the same format (millionm^3  and dimensionless)
# * Kernel used: python27
# * Date created: 20171124  

# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[40]:

EE_PATH = "projects/WRI-Aquaduct/PCRGlobWB20V08"

AREA_IMAGE_FILE_NAME = "area_5min_m2V11"


# Unfortunately specifying the dimensions caused the script to crash (internal error on Google's side) Specify scale instead.

DIMENSION5MIN = {}
DIMENSION5MIN["x"] = 4320
DIMENSION5MIN["y"] = 2160

MAXPIXELS =1e10


# In[ ]:




# In[10]:

import ee
import re
import subprocess
from calendar import monthrange


# In[4]:

ee.Initialize()


# ICs not in right format: discharge (m^3 / s) and runoff (m/month or m/year)

# In[23]:

areaImage = ee.Image("%s/%s"%(EE_PATH,AREA_IMAGE_FILE_NAME))


# In[30]:

dimensions = "%sx%s" %(DIMENSION5MIN["x"],DIMENSION5MIN["y"])


# In[31]:

crsTransform = [
                0.0833333309780367,
                0,
                -179.99999491255934,
                0,
                -0.0833333309780367,
                90.00000254430942
              ]


# In[46]:

def readAsset(assetId):
    # this function will read both images and imageCollections 
    if ee.data.getInfo(assetId)["type"] == "Image":
        asset = ee.Image(assetId)
        assetType = "image"


    elif ee.data.getInfo(assetId)["type"] == "ImageCollection":
        asset = ee.ImageCollection(assetId)
        assetType = "imageCollection"
        
    else:
        print("error")        
    return {"assetId":assetId,"asset":asset,"assetType":assetType}

def fluxToVolume(image):
    image = ee.Image(image)
    # Flux to volumne in million m^3 / y|month
    newImage = image.multiply(areaImage).divide(1e6)
    newImage = newImage.copyProperties(image)
    newImage = newImage.set("units","millionm3")    
    return newImage

#def exportToAsset(image,description,assetId,dimensions,region,maxPixels):

def exportToAsset(image):
    #print(image.propertyNames().getInfo())
    task = ee.batch.Export.image.toAsset(
        image =  image,
        description = description,
        assetId = assetId,
        dimensions = dimensions,
        #scale = scale,
        crs = crs,
        crsTransform = crsTransform,
        #region = geometry.bounds().getInfo()['coordinates'][0],
        maxPixels = maxPixels
    )
    #print(assetId)
    task.start()
    return 1

def createImageCollections(imageCollectionName):
    command = "earthengine create collection %s/%s" %(EE_PATH,imageCollectionName)
    response = subprocess.check_output(command,shell=True)
    print(response)


# In[8]:

command = "earthengine ls %s" %(EE_PATH)


# In[11]:

assetList = subprocess.check_output(command,shell=True).splitlines()


# In[14]:

fileName = "global_historical_runoff_month_mmonth_5min_1958_2014"


# In[19]:

ic = ee.ImageCollection("%s/%s"%(EE_PATH,fileName)) 


# In[28]:

newIc = ic.map(fluxToVolume)


# In[32]:

imageCollectionName = "test"


# ## Create folder and collection if not exists

# In[42]:

command = "earthengine create folder %s" %(EE_PATH)


# In[43]:

response = subprocess.check_output(command,shell=True)


# In[44]:

createImageCollections(imageCollectionName)


# In[ ]:

description= "test"
assetId = "%sdimensions,region,maxPixels)


# In[ ]:




# In[ ]:




# In[ ]:

newIc.map(exportToAsset)

