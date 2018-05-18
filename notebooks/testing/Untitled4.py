
# coding: utf-8

# In[92]:

import pandas as pd
import numpy as np

d = {'age' : [21, 45, 45, 5],
     'salary' : [20, 40, 10, 100]}

df = pd.DataFrame(d)


# method 1
df['is_rich_method1'] = np.where(df['salary']>=50, 'yes', 'no')

# method 2
df['is_rich_method2'] = ['yes' if x >= 50 else 'no' for x in df['salary']]

# method 3
df['is_rich_method3'] = 'no'
df.loc[df['salary'] > 50,'is_rich_method3'] = 'yes'


# In[93]:

df


# In[ ]:



