
# coding: utf-8

# # Title
# 
# * Purpose of script: test python 27 environement against several libraries  
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170728
# 
# 

# In[4]:

packages = ["earth engine","gdal","geopandas","Arcgis"]


# In[2]:



try:
    from osgeo import gdal
    gdal =1
except:
    gdal = 0
    
    
    
    


# In[5]:

print(packages)


# In[ ]:



