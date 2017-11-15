
# coding: utf-8

# # Plot data in an automated way using cartopy
# 
# * Purpose of script: Create images to show the BWS variance over the year
# * Author: Rutger Hofste
# * Kernel used: python35 Cartopy
# * Date created: 20171110

# In[1]:

get_ipython().magic('matplotlib inline')


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
print(sys.version)
get_ipython().magic('matplotlib inline')


# ## Settings

# In[46]:

SCRIPT_NAME = "Y2017M11D10_RH_Vizualize_Cartopy_BWS_V01"

INPUT_VERSION_TABLE = 6
INPUT_VERSION_SHAPE = 1

OUTPUT_VERSION = 1

S3_INPUT_PATH_TABLE = "s3://wri-projects/Aqueduct30/processData/Y2017M10D04_RH_Threshold_WaterStress_V02/output/"
S3_INPUT_PATH_SHAPE = "s3://wri-projects/Aqueduct30/processData/Y2017M11D10_RH_Make_Geometry_Valid_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/%s/output/" %(SCRIPT_NAME)

EC2_INPUT_PATH = "/volumes/data/%s/input" %(SCRIPT_NAME)
EC2_OUTPUT_PATH = "/volumes/data/%s/output" %(SCRIPT_NAME)

INPUT_FILE_NAME_TABLE = "Y2017M10D04_RH_Threshold_WaterStress_V%0.2d" %(INPUT_VERSION_TABLE)
INPUT_FILE_NAME_SHAPE = "Y2017M11D10_RH_Make_Geometry_Valid_V%0.2d" %(INPUT_VERSION_SHAPE)

# Extent Spain
#x0 = -15
#x1 = 10
#y0 = 35
#y1 = 45

#Extent Ethiopia
x0 = 30
x1 = 50
y0 = 0
y1 = 20


# In[4]:

get_ipython().system('rm -r {EC2_INPUT_PATH} ')
get_ipython().system('rm -r {EC2_OUTPUT_PATH} ')


# In[5]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH} ')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH} ')


# In[6]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_TABLE} {EC2_INPUT_PATH} --recursive')


# In[7]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_SHAPE} {EC2_INPUT_PATH} --recursive')


# In[30]:

import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point, Polygon, MultiPolygon
from shapely.geometry.collection import GeometryCollection
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
from cartopy.io.shapereader import Reader
from cartopy.feature import ShapelyFeature, Feature
from cartopy.io.img_tiles import StamenTerrain
import folium


# In[9]:

categories = {0:{"Min":-9999,"Max":0,"Name":"Cat0","facecolor":"blue","edgecolor":"red","alpha":0.5},
              1:{"Min":0,"Max":1,"Name":"1) Low","facecolor":"#FFFF99","edgecolor":"black","alpha":0.5},
              2:{"Min":1,"Max":2,"Name":"2) Low to Medium","facecolor":"#FFE600","edgecolor":"black","alpha":0.5},
              3:{"Min":2,"Max":3,"Name":"3) Medium to high","facecolor":"#FF9900","edgecolor":"black","alpha":0.5},
              4:{"Min":3,"Max":4,"Name":"4) High ","facecolor":"#FF1900","edgecolor":"black","alpha":0.5},
              5:{"Min":4,"Max":5.01,"Name":"5) Extremely High ","facecolor":"#990000","edgecolor":"black","alpha":0.5},
              6:{"Min":5.01,"Max":9999,"Name":"Cat6","facecolor":"blue","edgecolor":"red","alpha":0.5}}


# In[10]:

def createExtent(x0,x1,y0,y1):
    coords = [(x0,y0), (x1,y0), (x1, y1), (x0, y1)]
    extent = Polygon(coords)
    extentGdf = gpd.GeoDataFrame()
    extentGdf.loc[0, 'geometry'] = extent
    return extentGdf

def removeInvalid(gdf):
    gdf2 = gdf.copy()
    gdf2["isValid"] = gdf.geometry.is_valid
    gdfOut = gdf[gdf2['isValid']==True]
    return gdfOut

def categorizeBWS_s(value,column,categories):
    facecolor = "#ff66cc"
    edgecolor = "#ff66cc"
    alpha = 1
    
    for key, category in categories.items():
        if (value>= category["Min"]) and (value < category["Max"]):
            facecolor = category["facecolor"]
            edgecolor = category["edgecolor"]
            alpha = category["alpha"]
    
    return facecolor , edgecolor, alpha

def addFeatures(features):
    for item in features.records():
        attributes = item.attributes
        geometry = item.geometry
        facecolor , edgecolor, alpha = categorizeBWS_s(attributes,column,categories)        
        feature = ShapelyFeature(geometry,ccrs.PlateCarree(),facecolor=facecolor,edgecolor=edgecolor,alpha=alpha)
        ax.add_feature(feature)
    


# In[47]:

extentGdf = createExtent(x0,x1,y0,y1)


# In[48]:

gjson = extentGdf.to_json()


# In[49]:

polygon = folium.features.GeoJson(gjson)


# In[50]:

mapa = folium.Map([y0+(y1-y0)/2, x0+(x1-x0)/2],
                  zoom_start=4,
                  tiles='cartodbpositron')
mapa.add_child(polygon)


# In[51]:

df = pd.read_pickle(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_TABLE+".pkl"))


# In[52]:

gdf = gpd.read_file(os.path.join(EC2_INPUT_PATH,INPUT_FILE_NAME_SHAPE+".shp"))


# In[53]:

gdf = gdf.set_index("PFAF_ID", drop=False)


# In[54]:

gdf2 = gdf.copy()


# In[55]:

#gdf3 = gdf2.loc[gdf2['PFAF_ID'].between(210000,220000)]


# In[56]:

#gdf4 = gdf2.loc[gdf2['PFAF_ID'].between(230000,240000)]


# In[57]:

gdf3 = gdf2.loc[gdf2['PFAF_ID'].between(110000,120000)]
gdf4 = gdf2.loc[gdf2['PFAF_ID'].between(170000,180000)]


# In[58]:

gdfRegion = gdf3.append(gdf4)


# In[59]:

gdfRegionValid = removeInvalid(gdfRegion)


# In[60]:

gdfRegionValid.plot()


# In[61]:

gdf01 = gdfRegionValid.merge(df)


# In[62]:

gdf01.dtypes


# Public read only key

# In[64]:

for month in range(1,13):
    print(month)
    column = "ws_s_month_Y2014M%02.d" %(month)
    outputFileName = "%sV%0.2d.png" %(column,OUTPUT_VERSION)
    outputFilePath = os.path.join(EC2_OUTPUT_PATH,outputFileName)
    
    fig = plt.figure(figsize=(16, 9))
    ax = plt.axes(projection=ccrs.Mercator())
    extents = [x0,x1,y0,y1]
    ax.set_extent(extents, crs=None)
    #ax.coastlines(resolution='50m',alpha=1)

    
    tiler = StamenTerrain()
    mercator = tiler.crs
    ax.add_image(tiler, 6)
    
    ax.set_xmargin(0.00)
    ax.set_ymargin(0.00)
    
    for index, row in gdf01.iterrows():
        BWSvalue = row[column]
        facecolor , edgecolor, alpha = categorizeBWS_s(BWSvalue,COLUMN,categories)
        if row["aridAndLowWW_month_Y2014M%0.2d" %(month)]:
            facecolor = "#808080"
                
        feature = ShapelyFeature([row.geometry],ccrs.PlateCarree(),facecolor=facecolor,edgecolor=edgecolor,alpha=alpha)
        ax.add_feature(feature)
        
    plt.savefig(outputFilePath)
    


# In[65]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive')


# In[ ]:



