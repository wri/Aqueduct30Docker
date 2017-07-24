
# coding: utf-8

# In[17]:

import os


# In[18]:

a = os.environ['message']


# In[19]:

os.environ['message'] = 'New Message'


# In[20]:

get_ipython().system('echo $message')


# In[ ]:



