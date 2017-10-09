
# coding: utf-8

# # Create Line shapefile from CSV File 
# 
# * Purpose of script: Create a shapefile to visualize the flow network
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20171009

# In[11]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

from osgeo import ogr


# In[3]:

def myLine(coords):
    line = ogr.Geometry(type=ogr.wkbLineString)
    for xy in coords:
        line.AddPoint_2D(xy[0],xy[1])
    return line


# In[10]:

driver = ogr.GetDriverByName('ESRI Shapefile')
datasource = driver.CreateDataSource('/volumes/data/createLine/testlines.shp')


# In[ ]:




# In[ ]:




# In[5]:

coords = [(0,1),(1,1)]


# In[7]:

line = myLine(coords)


# In[8]:

print(line)


# In[ ]:



