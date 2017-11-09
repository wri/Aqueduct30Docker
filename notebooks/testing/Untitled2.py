
# coding: utf-8

# In[7]:

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import numpy as np
from cartopy.io import shapereader


# In[3]:

get_ipython().magic('matplotlib inline')


# In[6]:

ax = plt.axes(projection=ccrs.Robinson())
ax.set_global()

ax.stock_img()
ax.coastlines()
plt.plot(-0.08, 51.53, 'o', transform=ccrs.PlateCarree())
plt.plot([-0.08, 132], [51.53, 43.17], transform=ccrs.PlateCarree())
plt.savefig("test.png")


# In[5]:




# In[ ]:



