
# coding: utf-8

# In[2]:

import pandas as pd


# In[3]:

last_date = df.iloc[-1]
print(last_date)
last_unix = last_date.Timestamp
# one_day = 86400
one_minute = 60
next_unix = last_unix + one_minute
matplotlib.rc('figure', figsize=(20, 10))
for i in forecast_set:
    next_date = datetime.datetime.fromtimestamp(next_unix)
#     next_date = next_unix
    next_unix += 60
    df.loc[next_date] = [np.nan for _ in range(len(df.columns)-1)]+[i]
#     print(next_unix)


# In[ ]:



