
# coding: utf-8

# In[41]:

import pandas as pd
import numpy as np
from sqlalchemy import *


DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"
TABLE_NAME_RAW = "test01_raw"
TABLE_NAME_MODIFIED = "test01_modified"

NODATA_VALUE = -9999

d = {'integer_with_nan' : [21, 45, 45, np.NaN],
     'integer_without_nan' : [21, 45, 45, 0],
     'float_with_nan' : [20.2, 40.3, np.NaN, 1000],
     'float_without_nan' : [20.2, 40.3, 66.7, 1000],
     'string_without_nan' : ["foo","bar","fooz","bars"],
     'string_with_nan' : [np.NaN,"bar","fooz","bars"]}

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()

sql = "DROP TABLE IF EXISTS {}".format(TABLE_NAME_RAW)
result = engine.execute(sql)
sql = "DROP TABLE IF EXISTS {}".format(TABLE_NAME_MODIFIED)
result = engine.execute(sql)


df_raw = pd.DataFrame(d)
df_raw.to_sql(TABLE_NAME_RAW,connection)
df_modified = df_raw.fillna(NODATA_VALUE)
df_modified["integer_with_nan"] = df_modified["integer_with_nan"].astype(np.int64)
df_modified.to_sql(TABLE_NAME_MODIFIED,connection)

print("df_raw datatypes: \n", df_raw.dtypes)

print("\n \n df_modified datatypes: \n",df_modified.dtypes)

