
# coding: utf-8

# In[1]:

import mapboxgl
import pandas as pd
import geopandas as gpd
import geojson
import getpass


# In[2]:

# public api token
token = "pk.eyJ1IjoicnNiYXVtYW5uIiwiYSI6IjdiOWEzZGIyMGNkOGY3NWQ4ZTBhN2Y5ZGU2Mzg2NDY2In0.jycgv7qwF8MMIWt4cT0RaQ"


# In[4]:

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


# In[24]:

# adjust view angle
viz.bearing = -15
viz.pitch = 45

# add extrusion to viz using interpolation keyed on density in GeoJSON features
viz.height_property = 'density'
viz.height_stops = mapboxgl.utils.create_numeric_stops([0, 50, 100, 500, 1500, 5000], 0, 500000)
viz.height_function_type = 'interpolate'

# render again
viz.show()


# In[27]:

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

# In[5]:

# load a sample geodataframe
gdf = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))


# In[7]:




# In[12]:

gdf.to_file("test.geojson",driver="GeoJSON",encoding = 'utf-8')


# In[22]:

# create choropleth from polygon features stored as GeoJSON
viz2 = mapboxgl.viz.ChoroplethViz(data = 'test.geojson',
                     color_property='pop_est',
                     opacity=0.8,
                     center=(-96, 37.8),
                     zoom=3,
                     below_layer='waterway-label',
                     access_token = token
                    )
viz2.show()


# In[19]:

help(mapboxgl.utils.df_to_geojson)


# In[ ]:



