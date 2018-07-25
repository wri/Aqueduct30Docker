
# coding: utf-8

# This is a manual step. Using QGIS Hydrobasin level 6 basins have been assigned a delta_id.
# this delta_id corresponds to the delta_id in the GDBD database from previous steps. 
# The result are uploaded to S3

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[ ]:



