
# coding: utf-8

# In[2]:

from osgeo import ogr


# In[3]:

def myLine(coords):
    line = ogr.Geometry(type=ogr.wkbLineString)
    for xy in coords:
        line.AddPoint_2D(xy[0],xy[1])
    return line


# In[5]:

coords = [(0,1),(1,1)]


# In[7]:

line = myLine(coords)


# In[8]:

print(line)


# In[ ]:



