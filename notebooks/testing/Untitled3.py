
# coding: utf-8

# In[2]:

get_ipython().magic('matplotlib inline')


# In[3]:

from matplotlib import pyplot as plt
import geopandas as gpd
from shapely.geometry import Polygon

polys1 = gpd.GeoSeries([Polygon([(0,0), (2,0), (2,2), (0,2)]),
                         Polygon([(2,2), (4,2), (4,4), (2,4)])])

polys2 = gpd.GeoSeries([Polygon([(1,1), (3,1), (3,3), (1,3)]),
                         Polygon([(3,3), (5,3), (5,5), (3,5)])])

df1 = gpd.GeoDataFrame({'geometry': polys1, 'df1':[1,2]})
df2 = gpd.GeoDataFrame({'geometry': polys2, 'df2':[1,2]})

res_union = gpd.overlay(df1, df2, how='union')
res_union.plot()


# In[4]:

poly_union = gpd.GeoSeries([Polygon([(0,0), (0,2), (1,2), (1,3),     (2,3), (2,4), (3, 4), (3, 5), (5, 5), (5, 3), (4, 3), (4, 2),     (3,2), (3,1), (2, 1), (2, 0), (0, 0)])])

poly_union.plot(color = 'red')
plt.show()


# In[5]:

poly1 = df1['geometry']; poly2 = df2['geometry']
mergedpoly = poly1.union(poly2)
mergedpoly.plot()


# In[ ]:



