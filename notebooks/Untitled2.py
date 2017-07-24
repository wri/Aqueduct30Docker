
# coding: utf-8

# In[2]:

import os
os.environ['message']


# In[3]:

os.environ['message'] = 'created new message'


# In[4]:

get_ipython().system('echo $message')


# In[ ]:



