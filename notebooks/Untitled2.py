
# coding: utf-8

# In[1]:

import os


# In[2]:

print(os.environ["message"])


# In[4]:

os.environ["message"] = "test3"


# In[5]:

print(os.environ["message"])


# In[6]:

get_ipython().system(' echo $message')


# In[ ]:



