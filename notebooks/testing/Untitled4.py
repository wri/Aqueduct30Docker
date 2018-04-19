
# coding: utf-8

# In[1]:

import time
import json
from shapely.geometry import box
from gbdxtools import Interface
gbdx = Interface()

              


# In[2]:

def search_unordered(bbox, _type, count=100, cloud_cover=10):
    aoi = box(*bbox).wkt
    query = "item_type:{} AND item_type:DigitalGlobeAcquisition".format(_type)
    query += " AND attributes.cloudCover_int:<{}".format(cloud_cover)
    return gbdx.vectors.query(aoi, query, count=count)

              


# In[4]:

bbox = [-107.016792,38.852676,-106.894398,38.929903]
records = search_unordered(bbox, 'WV02')
ids = [r['properties']['attributes']['catalogID'] for r in records]
print(ids[3])

              


# In[7]:

len(records)


# In[8]:

def order(img_id):
    order = gbdx.Task("Auto_Ordering", cat_id=img_id)
    order.impersonation_allowed = True
    wf = gbdx.Workflow([order])
    wf.execute()
    return wf

              


# In[9]:

wfs = [order(w) for w in ids]


# In[ ]:



