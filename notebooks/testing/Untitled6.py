
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
from sqlalchemy import *

DATABASE_ENDPOINT = "aqueduct30v04.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
TABLE_NAME = "test01"

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

d1 = {'id' : [1, 2, 3],
     'foo' : [20, 40, 10]}

d2 = {'id' : [1, 2, 4],
     'bar' : [21, 42, 13]}


df1 = pd.DataFrame(d1)
df2 = pd.DataFrame(d2)


# In[2]:

df_merged = df1.merge(df2,on="id",how="outer")


# In[3]:

df_merged


# In[4]:

df1.dtypes


# In[5]:

df_merged.dtypes


# In[6]:

#column types converted to float due to Nans


# In[7]:

# cannot store as integer
# df_merged["bar"] = df_merged["bar"].astype(np.int64)


# In[8]:

df_merged.to_sql("test01",engine,if_exists='replace', index=False,chunksize=100)


# In[9]:

# Suggested by coldspeed
df_merged.astype(object).to_sql("test02",engine,if_exists='replace', index=False,chunksize=100)


# In[16]:

test = df_merged.astype(object)


# In[18]:




# In[10]:

# Suggested by Ami Tavory, 
df_merged2 = df_merged.copy()
df_merged2["foo"].fillna(-9999, inplace=True)
df_merged2["bar"].fillna(-9999, inplace=True)


# In[11]:

df_merged2


# In[12]:

df_merged2["foo"] = df_merged2["foo"].astype(np.int64)
df_merged2["bar"] = df_merged2["foo"].astype(np.int64)


# In[13]:

df_merged2.dtypes


# In[14]:

df_merged2.to_sql("test03",engine,if_exists='replace', index=False,chunksize=100)

