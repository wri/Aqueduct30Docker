
# coding: utf-8

# In[18]:

import pandas as pd
import numpy as np
from sqlalchemy import *

d = {'age' : [21, 45, 45, 5],
     'salary' : [20.00, 40.30, 10.89, 100.03],
     'name': ["foo","bar","fooz","bars"]}

df = pd.DataFrame(d)

"""
Docs at https://pandas.pydata.org/pandas-docs/stable/comparison_with_sql.html
"""


# In[8]:

df


# In[14]:

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
TABLE_NAME = "test02"


# In[15]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

sql = text("DROP TABLE IF EXISTS {};".format(TABLE_NAME))
result = engine.execute(sql)


# In[16]:

df.to_sql(TABLE_NAME,connection)


# In[17]:

df_out = df.copy()


# In[25]:

# Create new column based on other columns

df_out["tax"] = df_out.loc[df_out['salary'] > 18, 'salary']*0.3



# In[39]:

df_out


# In[31]:

sql01 = "ALTER TABLE test02 ADD COLUMN tax double precision" 


# In[32]:

result = engine.execute(sql01)


# In[33]:

sql02 = "UPDATE test02 SET tax = salary*0.3 WHERE salary > 18;" 


# In[34]:

result = engine.execute(sql02)


# In[35]:

sql03 = "SELECT * FROM test02"


# In[37]:

df_from_sql = pd.read_sql(sql03,connection)


# In[38]:

df_from_sql


# In[ ]:



