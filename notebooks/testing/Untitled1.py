
# coding: utf-8

# In[19]:

import numpy as np
import pandas as pd
import scipy.stats as stats
import matplotlib as plt
from sklearn import linear_model

get_ipython().magic('matplotlib inline')



# In[41]:

d = {'integer_with_nan' : [21, 45, 45, np.NaN], 
     'integer_without_nan' : [21, 45, 45, 0], 
     'float_with_nan' : [20.2, 40.3, np.NaN, 1000], 
     'float_without_nan' : [20.2, 40.3, 66.7, 1000], 
     'string_without_nan' : ["foo","bar","fooz","bars"], 
     'string_with_nan' : [np.NaN,"bar","fooz","bars"],
     'simple_x': [1960,1961,1962,1970],
     'simple_y': [10.1,40.6,40.6,80.7]}


# In[42]:

df = pd.DataFrame(d)


# In[43]:

df.head()


# In[44]:

ax1 = df.plot.scatter("simple_x","simple_y")
ax1.set_ylim(df["simple_y"].min(),df["simple_y"].max())


# In[45]:

lm = linear_model.LinearRegression()


# In[46]:

features = pd.DataFrame(df["simple_x"])


# In[47]:

y = pd.DataFrame(df["simple_y"])


# In[48]:

lm.fit(features,y)


# In[49]:

lm.coef_


# In[50]:

lm.intercept_


# In[ ]:



