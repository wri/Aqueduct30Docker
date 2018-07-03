
# coding: utf-8

# Exploring Geodataframes in Maps
# 
# Pandas is the de facto package of choice for most data scientist working with python. Geopandas adds great geospatial components to the pandas framework while maintaining its simplicity. There is however one challenge with geopandas: Interactively exploring the data on a map. 
# 
# This notebook explores several options to interactively visualize the data on a map. 
# 
# Sharing results with my team I've reverted back to saving the geodataframe as a shapefile and let them explore the results rlocally. There miust be a better way though that lives entirely inside the Jupyter Notebook environment. 
# 

# # Native Geopandas

# In[30]:

get_ipython().magic('matplotlib inline')
import geopandas as gpd


# In[25]:

# load a sample geodataframe
gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))


# In[26]:

gdf.shape


# In[28]:

gdf.head()


# In[31]:

gdf.plot()


# In[ ]:

T


# In[ ]:




# In[ ]:

import folium
import branca


# In[24]:




m = folium.Map(location = [52,4],
        zoom_start=6)


# In[13]:




# In[14]:

geojson = world.to_json()


# In[18]:

features = folium.features.GeoJson(geojson)


# In[23]:

m.add_child(features)


# In[ ]:

htmlTable = dfTemp.to_html()
iFrame = branca.element.IFrame(htmlTable, width=width, height=height)


# In[ ]:




# In[ ]:

def shapelyToFoliumFeature(row):
    """converts a shapely feature to a folium (leaflet) feature. row needs to have a geometry column. CRS is 4326
    
    Args:
        row (geoPandas.GeoDataFrame row) : the input geodataframe row. Appy this function to a geodataframe gdf.appy(function, 1)
        
    Returns:
        foliumFeature  (folium feature) : foliumFeature with popup child.
    
    """    

    width, height = 310,110
    dfTemp = pd.DataFrame(row.drop("geometry"))
    htmlTable = dfTemp.to_html()
    iFrame = branca.element.IFrame(htmlTable, width=width, height=height)
    geoJSONfeature = geojson.Feature(geometry=row["geometry"], properties={})
    foliumFeature = folium.features.GeoJson(geoJSONfeature)
    foliumFeature.add_child(folium.Popup(iFrame))
    return foliumFeature 


# In[ ]:




# In[ ]:




# In[ ]:



