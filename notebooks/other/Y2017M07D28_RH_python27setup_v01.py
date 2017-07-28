
# coding: utf-8

# # Title
# 
# * Purpose of script: test python 27 environement against several libraries  
# * Author: Rutger Hofste
# * Kernel used: python27
# * Date created: 20170728
# 
# 

# In[ ]:

packages = {""}


# In[2]:



try:
    from osgeo import gdal
    gdal =1
except:
    gdal = 0
    
    
    
    


# In[3]:

print(gdal)


# In[ ]:



