
# coding: utf-8

# In[2]:

""" Inspect result for water stress basins.
-------------------------------------------------------------------------------

Inspect the results of the aggregation by deltas on the water stress levels. 

- merge delta and pfaf data
- upload to bq
- inspect differences


Author: Rutger Hofste
Date: 20180730
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = 'Y2018M07D30_RH_QA_Deltas_Water_Stress_V01'
OUTPUT_VERSION = 1

RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"




# In[ ]:



