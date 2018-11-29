
# coding: utf-8

# In[1]:

""" Union of hydrobasin and GADM 36 level 1 using geopandas parallel processing.
-------------------------------------------------------------------------------

Step 1:
Create polygons (10x10 degree, 648).

Step 2:
Clip all geodataframes with polygons (intersect)

Step 3:
Peform union per polygon

Step 4: 
Merge results into large geodataframe

Step 5:
Dissolve on unique identifier.







Author: Rutger Hofste
Date: 20181128
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""


# In[ ]:



