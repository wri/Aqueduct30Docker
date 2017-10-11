
# coding: utf-8

# In[1]:

import pandas as pd
import re
import numpy as np


# In[2]:

index = [1,2,3,4]


# In[3]:

d = {'capped1': [1,0,-1,np.nan], 'capped2': [2,0,np.nan,-9999],'signed':[2,0,-3,np.nan]}


# In[4]:

df = pd.DataFrame(data=d, index=index)


# In[5]:

df


# In[6]:

df_right = df.filter(regex=("capped*")).clip(lower=0)


# In[7]:

df_left = df.drop(list(df_right.columns), 1)


# In[8]:

df_out = df_left.merge(df_right,left_index=True,right_index=True,how="outer")


# In[9]:

df_out

