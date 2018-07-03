
# coding: utf-8

# In[15]:

import os
import mapbox


# In[16]:

F = open("/.mapbox","r")
token = F.read().splitlines()[0]
F.close()


# In[20]:




# In[19]:

geocoder = mapbox.Geocoder()
geocoder.session.params['access_token'] == token


# In[21]:




# In[ ]:



