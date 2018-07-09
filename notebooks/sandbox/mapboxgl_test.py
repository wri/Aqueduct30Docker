
# coding: utf-8

# In[1]:

import mapboxgl
import pandas as pd
import geopandas as gpd
import json
import geojson
import getpass
import os


# In[2]:

F = open("/.mapbox_public","r")
token = F.read().splitlines()[0]
F.close()
os.environ["MAPBOX_ACCESS_TOKEN"] = token


# In[3]:

# create choropleth from polygon features stored as GeoJSON
viz = mapboxgl.viz.ChoroplethViz('https://raw.githubusercontent.com/mapbox/mapboxgl-jupyter/master/examples/data/us-states.geojson', 
                     color_property='density',
                     color_stops= mapboxgl.utils.create_color_stops([0, 50, 100, 500, 1500], colors='YlOrRd'),
                     color_function_type='interpolate',
                     line_stroke='--',
                     line_color='rgb(128,0,38)',
                     line_width=1,
                     opacity=0.8,
                     center=(-96, 37.8),
                     zoom=3,
                     below_layer='waterway-label',
                     access_token = token
                    )
viz.show()


# In[ ]:

# adjust view angle
viz.bearing = -15
viz.pitch = 45

# add extrusion to viz using interpolation keyed on density in GeoJSON features
viz.height_property = 'density'
viz.height_stops = mapboxgl.utils.create_numeric_stops([0, 50, 100, 500, 1500, 5000], 0, 500000)
viz.height_function_type = 'interpolate'

# render again
viz.show()


# In[ ]:

# must be JSON object (need to extend to use referenced JSON file)
data = [{"id": "01", "name": "Alabama", "density": 94.65}, {"id": "02", "name": "Alaska", "density": 1.264}, {"id": "04", "name": "Arizona", "density": 57.05}, {"id": "05", "name": "Arkansas", "density": 56.43}, {"id": "06", "name": "California", "density": 241.7}, {"id": "08", "name": "Colorado", "density": 49.33}, {"id": "09", "name": "Connecticut", "density": 739.1}, {"id": "10", "name": "Delaware", "density": 464.3}, {"id": "11", "name": "District of Columbia", "density": 10065}, {"id": "12", "name": "Florida", "density": 353.4}, {"id": "13", "name": "Georgia", "density": 169.5}, {"id": "15", "name": "Hawaii", "density": 214.1}, {"id": "16", "name": "Idaho", "density": 19.15}, {"id": "17", "name": "Illinois", "density": 231.5}, {"id": "18", "name": "Indiana", "density": 181.7}, {"id": "19", "name": "Iowa", "density": 54.81}, {"id": "20", "name": "Kansas", "density": 35.09}, {"id": "21", "name": "Kentucky", "density": 110}, {"id": "22", "name": "Louisiana", "density": 105}, {"id": "23", "name": "Maine", "density": 43.04}, {"id": "24", "name": "Maryland", "density": 596.3}, {"id": "25", "name": "Massachusetts", "density": 840.2}, {"id": "26", "name": "Michigan", "density": 173.9}, {"id": "27", "name": "Minnesota", "density": 67.14}, {"id": "28", "name": "Mississippi", "density": 63.5}, {"id": "29", "name": "Missouri", "density": 87.26}, {"id": "30", "name": "Montana", "density": 6.858}, {"id": "31", "name": "Nebraska", "density": 23.97}, {"id": "32", "name": "Nevada", "density": 24.8}, {"id": "33", "name": "New Hampshire", "density": 147}, {"id": "34", "name": "New Jersey", "density": 1189}, {"id": "35", "name": "New Mexico", "density": 17.16}, {"id": "36", "name": "New York", "density": 412.3}, {"id": "37", "name": "North Carolina", "density": 198.2}, {"id": "38", "name": "North Dakota", "density": 9.916}, {"id": "39", "name": "Ohio", "density": 281.9}, {"id": "40", "name": "Oklahoma", "density": 55.22}, {"id": "41", "name": "Oregon", "density": 40.33}, {"id": "42", "name": "Pennsylvania", "density": 284.3}, {"id": "44", "name": "Rhode Island", "density": 1006}, {"id": "45", "name": "South Carolina", "density": 155.4}, {"id": "46", "name": "South Dakota", "density": 98.07}, {"id": "47", "name": "Tennessee", "density": 88.08}, {"id": "48", "name": "Texas", "density": 98.07}, {"id": "49", "name": "Utah", "density": 34.3}, {"id": "50", "name": "Vermont", "density": 67.73}, {"id": "51", "name": "Virginia", "density": 204.5}, {"id": "53", "name": "Washington", "density": 102.6}, {"id": "54", "name": "West Virginia", "density": 77.06}, {"id": "55", "name": "Wisconsin", "density": 105.2}, {"id": "56", "name": "Wyoming", "density": 5.851}, {"id": "72", "name": "Puerto Rico", "density": 1082}]

# create choropleth map with vector source styling use data in JSON object
viz = mapboxgl.viz.ChoroplethViz(data, 
                    vector_url='mapbox://mapbox.us_census_states_2015',
                    vector_layer_name='states',
                    vector_join_property='STATE_ID',
                    data_join_property='id',
                    color_property='density',
                    color_stops=mapboxgl.utils.create_color_stops([0, 50, 100, 500, 1500], colors='YlOrRd'),
                    line_stroke='dashed',
                    line_color='rgb(128,0,38)',
                    opacity=0.8,
                    center=(-96, 37.8),
                    zoom=3,
                    below_layer='waterway-label',
                    access_token = token
                   )
viz.show()


# In[ ]:




# Let's try with our own geojson file
# 

# In[4]:

# load a sample geodataframe
gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))


# In[5]:

gdf.crs = None


# In[6]:

gdf.drop("name",axis=1,inplace=True)


# In[7]:

gjson = gdf.to_json()


# In[8]:

gjson_valid = geojson.loads(gjson)


# In[ ]:

# create choropleth from polygon features stored as GeoJSON
viz2 = mapboxgl.viz.ChoroplethViz(data=gjson_valid,
                     vector_layer_name='test',
                     below_layer='waterway-label',
                     color_property='pop_est',
                     color_stops= mapboxgl.utils.create_color_stops([0, 100000, 1e9], colors='YlOrRd'),
                     color_function_type='interpolate',
                     opacity=0.8,
                     center=(-96, 37.8),
                     zoom=3,
                     access_token = token
                    )
viz2.style='mapbox://styles/mapbox/dark-v9?optimize=true'
viz2.show()


# # Tileset from Mapbox, properties from pandas

# In[9]:

data_url = 'https://raw.githubusercontent.com/mapbox/mapboxgl-jupyter/master/examples/data/2010_us_population_by_postcode.csv'
df = pd.read_csv(data_url).round(3)
df.head(2)


# In[10]:

# Group pandas dataframe by a value
measure = '2010 Census Population'
dimension = 'Zip Code ZCTA'

data_temp = df[[dimension, measure]].groupby(dimension, as_index=False).mean()
color_breaks = [round(data_temp[measure].quantile(q=x*0.1), 2) for x in range(2,11)]
color_stops = mapboxgl.utils.create_color_stops(color_breaks, colors='PuRd')
data = json.loads(data_temp.to_json(orient='records'))


# In[ ]:




# In[ ]:

# Create the viz
viz3 = mapboxgl.viz.ChoroplethViz(data, 
                                  vector_url='mapbox://rsbaumann.bv2k1pl2',
                                  vector_layer_name='2016_us_census_postcode',
                                  vector_join_property='postcode',
                                  data_join_property=dimension,
                                  color_property=measure,
                                  color_stops=color_stops,
                                  line_color = 'rgba(0,0,0,0.05)',
                                  line_width = 0.5,
                                  opacity=0.7,
                                  center=(-95, 45),
                                  zoom=2,
                                  below_layer='waterway-label'
                                  )
viz3.show()


# In[11]:

# With my own vectortiles


# In[12]:

df_test = pd.read_csv("test.csv")


# In[13]:

df_test


# In[14]:

df_test = df_test.rename(columns={"PFAF_ID":"pfaf_id"})


# In[ ]:




# In[15]:

color_stops_test = mapboxgl.utils.create_color_stops([0, 2, 11], colors='YlOrRd')


# In[ ]:




# In[16]:

data_test = json.loads(df_test.to_json(orient='records'))


# In[ ]:




# In[19]:

# Create the viz
viz4 = mapboxgl.viz.ChoroplethViz(data = data_test, 
                                  vector_url='mapbox://rutgerhofste.hybas06_v04_V04',
                                  vector_layer_name='hybas06_v04', # Warning should match name on mapbox.
                                  vector_join_property='pfaf_id',
                                  data_join_property="pfaf_id",
                                  color_property="valuez",
                                  color_stops= color_stops_test,
                                  line_color = 'rgba(0,0,0,0.05)',
                                  line_width = 0.5,
                                  opacity=0.7,
                                  center=(5, 52),
                                  zoom=5,
                                  below_layer='waterway-label'
                                  )
viz4.show()


# In[ ]:

viz

