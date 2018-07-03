
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

# In[1]:

get_ipython().magic('matplotlib inline')
import geopandas as gpd


# In[2]:

# load a sample geodataframe
gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))


# In[ ]:

gdf.shape


# In[ ]:

gdf.head()


# In[ ]:

gdf.plot(column='gdp_md_est',cmap='OrRd');


# The result is a static map with no tooltip. You can do much [more](http://geopandas.org/mapping.html) with the native plotting command but it's based on matplotlib and therefore not interactive. 

# # Folium
# 
# [Folium](https://github.com/python-visualization/folium) is leaflet maps plus Python data. based on [this](https://ocefpaf.github.io/python4oceanographers/blog/2015/12/14/geopandas_folium/) tutorial from 2015. 
# 
# Limitations:
# * All data processing is done client side. This solution does not leverage serverless architecure, vectortiles etc. Therefore, use only with relatively simple geometries. 
# * By default runs on one CPU. 
# 
# 

# In[7]:

import folium
import branca
import pandas as pd
import geojson


# In[ ]:

gjson = gdf.to_json()


# In[ ]:

m = folium.Map(location = [52,4],
               zoom_start=6)


# In[ ]:

features_1 = folium.features.GeoJson(gjson)
m.add_child(features_1)


# ## Popups
# 
# 
# this is a nice first step but you want to inspect the underlying data. This involves a couple more steps and requires a per feature approach which is much more computationally demanding. The process can be paralellized though. 
# 

# In[ ]:

def pre_process_gdf(gdf_in):    
    """Assert the crs is set to 4326 and geometry column name is 'geometry'
    """
    gdf = gdf_in.copy()
    gdf = gdf.to_crs(epsg='4326')
    gdf = gdf.rename(columns={gdf.geometry.name:"geometry"})
    return gdf    


# In[50]:

def row_geojson_feature(row):    
    geojson_feature = geojson.Feature(geometry=row["geometry"], properties={})
    return geojson_feature


# In[ ]:

def row_popup(row):
    width, height = 200,200
    attributes = pd.DataFrame(row.drop("geometry"))
    html_table = attributes.to_html()
    i_frame = branca.element.IFrame(html_table, width=width, height=height)
    return i_frame


# In[ ]:

def zip_features_popups(row):
    folium_feature = folium.features.GeoJson(row["gjson"])
    folium_feature.add_child(folium.Popup(row["i_frame"]))
    return folium_feature


# In[ ]:

gdf_temp = gdf.copy()


# In[ ]:

# Generate geosjon representations of geometries
gdf_temp["gjson"] = gdf_temp.apply(row_geojson_feature,axis=1)


# In[ ]:

# Create iframes as popups. 
gdf_temp["i_frame"] = gdf.apply(row_popup,1)


# In[ ]:

# Zip geosjon features and popups in folium features (This step is very slow, todo: look into speed ups.)
gdf_temp["features"] = gdf_temp.apply(zip_features_popups,axis=1)


# In[ ]:

# Create folium feature group
feature_group_1 = folium.FeatureGroup(name="test")


# In[ ]:

features_list = gdf_temp["features"].tolist()


# In[ ]:

for feature in features_list:
    feature.add_to(feature_group_1)


# In[ ]:

m2 = folium.Map(location = [52,4],
               zoom_start=6)
m2.add_child(feature_group_1)
m2


# Now everything in one function that calls the helper functions. 

# In[ ]:

def gdf_to_feature_group(gdf,name="noname"):
    """ 
    Converts a geodataframe into a folium featuregroup.
    
    Usage: you can add the folium feature group to a map by
    
    m = folium.Map(location = [52,4],
                   zoom_start=6)
    m.add_child(feature_group)
    m.add_child(folium.LayerControl())
    
    
    Args:
        gdf (gpd.GeoDataFrame) : Geodataframe.
        name (string) : Name for folium group.
    
    Returns:
        feature_group (folium feature group): folium feature group.
    
    """
    
    gdf_clean = pre_process_gdf(gdf)
    gdf_temp = gdf_clean.copy()
    # Generate geosjon representations of geometries
    gdf_temp["gjson"] = gdf_clean.apply(row_geojson_feature,1)
    gdf_temp["i_frame"] = gdf_clean.apply(row_popup,1)
    gdf_temp["features"] = gdf_temp.apply(zip_features_popups,axis=1)
    
    # Add folium features to folium group
    feature_group_2 = folium.FeatureGroup(name=name)
    features_list = gdf_temp["features"].tolist()
    for feature in features_list:
        feature.add_to(feature_group_2)
    return feature_group_2


# In[ ]:

# load a new sample geodataframe
gdf2 = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
feature_group_2 = gdf_to_feature_group(gdf2,name="countries")


# In[ ]:

try:
    del(m3)
except:
    pass
m3 = folium.Map(location = [52,4],
               zoom_start=6)
m3.add_child(feature_group_2)
m3


# ## Style
# 
# 

# In[54]:

# load a sample geodataframe
gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
gdf_temp = gdf.copy()


# In[55]:

gjson = gdf.to_json()


# In[56]:

gdf.describe()


# In[57]:

max_colorbar = gdf["pop_est"].max()


# In[58]:

colorscale = branca.colormap.linear.YlOrRd.scale(0, max_colorbar)


# In[59]:

colorscale


# In[60]:

def row_style(row):
    style_dict = {'fillColor': "black",
                   'color': 'black',
                   'weight': 2,
                   'dashArray': '5, 5'
                 }
    return row_style

    


# In[61]:

# Create style columns
gdf_temp["style"] = gdf.apply(row_style,1)


# In[62]:

# Generate geosjon representations of geometries
gdf_temp["gjson"] = gdf_temp.apply(row_geojson_feature,axis=1)


# In[63]:

gdf_temp.head()


# In[64]:

def zip_features(row):
    folium_feature = folium.features.GeoJson(data = row["gjson"],
                                             style_function = row["style"])
    
    #folium_feature.add_child(folium.Popup(row["i_frame"]))
    return folium_feature


# In[39]:




# In[41]:

features_1 =folium.GeoJson(data= gjson,
                           style_function=lambda feature: {
                                'fillColor': my_color_function(feature),
                                'color': 'black',
                                'weight': 2,
                                'dashArray': '5, 5'
                            })


# In[36]:

features_1 = folium.features.GeoJson(data= gjson,
                                     style_function = style_function)
                                 


# In[42]:

try:
    del(m)
except:
    pass
m = folium.Map(location = [52,4],
               zoom_start=6)
m.add_child(features_1)
m


# # Vision
# 
# as a data scientist I would like to see the integration of GIS capabilities (postGIS) and serverless database architectures such as Google BigQuery and Amazon Redshift. A python Library could query the database by using either SQL or a python pandas method e.g. df.loc[df["colum"== value]].
# 
# A simplified version (vector tiles) of the geometry is parsed to the client depending on bounding box and zoom level. Data is added to tooltips directly. Visualization can be done in a leaflet layer.  
# 
# 

# In[ ]:



