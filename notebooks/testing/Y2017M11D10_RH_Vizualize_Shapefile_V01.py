
# coding: utf-8

# In[1]:

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
from cartopy.io.shapereader import Reader
from cartopy.feature import ShapelyFeature
import geopandas as gpd


# In[2]:

get_ipython().magic('matplotlib inline')


# In[ ]:




# In[3]:

fname = "/volumes/repos/Aqueduct30Docker/notebooks/testing/samplefiles/testShape.shp"
column = "PFAF_ID"

categories = {0:{"Min":-9999,"Max":0,"Name":"Cat0","facecolor":"blue","edgecolor":"red","alpha":0},
              1:{"Min":0,"Max":1,"Name":"Cat1","facecolor":"blue","edgecolor":"red","alpha":0},
              2:{"Min":1,"Max":2,"Name":"Cat2","facecolor":"blue","edgecolor":"red","alpha":0},
              3:{"Min":2,"Max":3,"Name":"Cat3","facecolor":"blue","edgecolor":"red","alpha":0},
              4:{"Min":3,"Max":4,"Name":"Cat4","facecolor":"blue","edgecolor":"red","alpha":0},
              5:{"Min":4,"Max":5,"Name":"Cat5","facecolor":"blue","edgecolor":"red","alpha":0},
              6:{"Min":5,"Max":9999,"Name":"Cat6","facecolor":"blue","edgecolor":"red","alpha":0}}



# In[9]:

def categorizeBWS_s(attributes,column,categories):
    facecolor = "#ff66cc"
    edgecolor = "#ff66cc"
    alpha = 1
    
    score = attributes[column]
    for key, category in categories.items():
        if (score >= category["Min"]) and (score < category["Min"]):
            facecolor = category["facecolor"]
            edgecolor = category["edgecolor"]
            alpha = category["alpha"]
    
    return facecolor , edgecolor, alpha
    
    


# In[10]:

def addFeatures(fname):
    f = Reader(fname)
    for item in f.records():
        attributes = item.attributes
        geometry = item.geometry
        facecolor , edgecolor, alpha = categorizeBWS_s(attributes,column,categories)
        
        feature = ShapelyFeature(geometry,ccrs.PlateCarree(),facecolor=facecolor,edgecolor=edgecolor,alpha=alpha)
        ax.add_feature(feature)


# In[11]:

fig = plt.figure(figsize=(20, 100))
ax = plt.axes(projection=ccrs.Robinson())
extents = [-20,20,30,40]
ax.set_extent(extents, crs=None)
ax.coastlines(resolution='50m',alpha=1)

addFeatures(fname)





# In[ ]:


    


# In[ ]:

f = Reader(fname)
for item in test.records():
    attributes = item.attributes
    geometry = item.geometry
    feature = ShapelyFeature(geometry,ccrs.PlateCarree())
    ax.add_feature(feature)
    


# In[ ]:

# Using Cartopy


shape_feature = ShapelyFeature(Reader(fname).geometries(),
                                ccrs.PlateCarree(), facecolor='blue',edgecolor='red')


ax.add_feature(shape_feature)





# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

fig = plt.figure(figsize=(20, 100))
ax = plt.axes(projection=ccrs.Robinson())
extents = [-20,20,30,40]
ax.coastlines(resolution='50m')
ax.set_extent(extents, crs=None)


test = Reader(fname)
test.records()
for item in test.records():
    attributes = item.attributes
    geometry = item.geometry
    print(type(geometry))
    feature = ShapelyFeature(geometry,ccrs.PlateCarree())
    ax.add_feature(feature)

    
    


# In[ ]:

shape_feature = ShapelyFeature(Reader(fname).geometries(),
                                ccrs.PlateCarree(), facecolor='blue',edgecolor='red')

