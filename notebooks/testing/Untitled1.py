
# coding: utf-8

# In[1]:

from calendar import monthrange, isleap


# In[12]:

year = 1980


# In[13]:

daysPerYear = 366 if isleap(year) else 365


# In[14]:

print(daysPerYear)


# In[ ]:



