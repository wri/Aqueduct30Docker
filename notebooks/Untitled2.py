
# coding: utf-8

# In[2]:

import os
os.environ['message']


# In[5]:

os.environ['message'] = str('created new message')


# In[6]:

get_ipython().system('echo $message')


# In[ ]:



