
# coding: utf-8

# # Y2017M12D19_RH_PostGis_EEFC_EE_V01
# 
# * Purpose of script: fiddle to see if using postGIS for EE is a good solution
# * Kernel used: python27
# * Date created: 20171219

# In[22]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[25]:

import ee
import folium
import folium_gee


# In[26]:

ee.Initialize()


# In[27]:

# Get an area to look at
lat = 39.495159
lon = -107.3689237
zoom_start=10

# Open Street Map Base
m = folium.Map(location=[lat, lon], tiles="OpenStreetMap", zoom_start=zoom_start)


# In[28]:

geometrySmall = ee.Geometry.Polygon(coords=[[-180.0, -81.0], [180,  -81.0], [180, 81], [-180,81]], proj= ee.Projection('EPSG:4326'),geodesic=False )


# In[ ]:



