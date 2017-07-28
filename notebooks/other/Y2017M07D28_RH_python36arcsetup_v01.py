
# coding: utf-8

# # Test Python 36 arc setup
# 
# * Purpose of script: test python 36 arc environement against several libraries  
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170728

# In[1]:

packages = {"earth engine":-1,"gdal":-1,"geopandas":-1,"arcgis":-1}


# In[2]:

try:
    import ee
    packages["earth engine"]=1
except:
    packages["earth engine"]=0  


# In[3]:

try:
    from osgeo import gdal
    packages["gdal"]=1
except:
    packages["gdal"]=0   


# In[4]:

try:
    import geopandas
    packages["geopandas"]=1
except:
    packages["geopandas"]=0 


# In[5]:

try:
    import arcgis.gis
    packages["arcgis"]=1
except:
    packages["arcgis"]=0 


# In[6]:

print(packages)

