
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from sqlalchemy import *


d = {'integer_with_nan' : [21, 45, 45, np.NaN],
     'integer_without_nan' : [21, 45, 45, 0],
     'float_with_nan' : [20.2, 40.3, np.NaN, 1000],
     'float_without_nan' : [20.2, 40.3, 66.7, 1000],
     'string_without_nan' : ["foo","bar","fooz","bars"],
     'string_with_nan' : [np.NaN,"bar","fooz","bars"]}


# In[2]:

df_raw = pd.DataFrame(d)


# In[ ]:

df_raw["valid_integer"] = df_raw["integer_with_nan"]

