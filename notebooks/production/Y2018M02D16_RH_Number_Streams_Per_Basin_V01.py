
# coding: utf-8

# ### Y2018M02D16_RH_Number_Streams_Per_Basin_V01
# 
# * Purpose of script: determine the number of streams per GDBD basin
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20180216
# 
# Approach:  
# 1. load line data  
# 1. buffer lines with tiny value
# 1. group lines based on overlap
# 1. Add group numbers to basin polygons
# 1. basins with more than 1 (>1) line groups are delta regions
# 
# Old approach:
# [Strategy](https://gis.stackexchange.com/questions/132723/unsplit-dissolve-multiple-touching-lines-in-stream-network-using-arcgis-desktop)
# 
# 1. Explode multilines into single lines 
# 1. Tiny buffer around single lines
# 1. Take Union
# 1. Spatial join single line geodataframe and dissolved ID's 
# 1. Aggregate using polyline ID from previous step
# 
# 
# 
# 
# 

# In[1]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[2]:

SCRIPT_NAME = "Y2018M02D16_RH_Number_Streams_Per_Basin_V01"

EC2_INPUT_PATH  = ("/volumes/data/{}/input/").format(SCRIPT_NAME)
EC2_OUTPUT_PATH = ("/volumes/data/{}/output/").format(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M02D15_RH_GDBD_Merge_V01/output/"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/{}/output".format(SCRIPT_NAME)


INPUT_VERSION = 6
OUTPUT_VERSION = 1

TESTING = 1


# In[ ]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[ ]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[3]:

import geopandas as gpd
import pandas as pd
import eeconvert
import folium


get_ipython().magic('matplotlib inline')


# In[4]:

file_name_streams = "{}GDBD_streams_EPSG4326_V{:02.0f}.shp".format(EC2_INPUT_PATH,INPUT_VERSION)
file_name_basins = "{}GDBD_basins_EPSG4326_V{:02.0f}.shp".format(EC2_INPUT_PATH,INPUT_VERSION)


# In[5]:

gdf_streams = gpd.GeoDataFrame.from_file(file_name_streams)
gdf_basins = gpd.GeoDataFrame.from_file(file_name_basins)


# In[6]:

gdf_streams_backup = gdf_streams.copy()
tiny_value = 0.00001

if TESTING:
    gdf_streams = gdf_streams[0:200]
    tiny_value = 0.1


# In[7]:

gdf_streams['GDBD_ID'] = gdf_streams['GDBD_ID'].astype('int64')


# In[8]:

gdf_streams.dtypes


# ## Functions 

# In[9]:

def explode(gdf):
    """
    Will explode the geodataframe's muti-part geometries into single
    geometries. Each row containing a multi-part geometry will be split into
    multiple rows with single geometries, thereby increasing the vertical size
    of the geodataframe. The index of the input geodataframe is no longer
    unique and is replaced with a multi-index.

    The output geodataframe has an index based on two columns (multi-index)
    i.e. 'level_0' (index of input geodataframe) and 'level_1' which is a new
    zero-based index for each single part geometry per multi-part geometry
        
    Args:
        gdf (gpd.GeoDataFrame) : input geodataframe with multi-geometries
        
    Returns:
        gdf (gpd.GeoDataFrame) : exploded geodataframe with each single
                                 geometry as a separate entry in the
                                 geodataframe. The GeoDataFrame has a multi-
                                 index set to columns level_0 and level_1
        
    """
    gs = gdf.explode()
    gdf2 = gs.reset_index().rename(columns={0: 'geometry'})
    gdf_out = gdf2.merge(right=gdf.drop('geometry', axis=1),
                         left_on='level_0',
                         right_index=True)
    gdf_out = (gdf_out.set_index(['level_0', 'level_1'])
                      .set_geometry('geometry'))
    gdf_out.crs = gdf.crs
    return gdf_out


def group_geometry(gdf, buffer_value=0.01, out_column_name="geometry_group"):
    """
    Adds a column to the dataframe with a geometry group number. The
    group number is determined by overlapping or touching geometries.
    A geodataframe will be exploded before assigning the group number.
    If the input geodataframe contains multi-geometries, the shape will
    increase. This function can also be used to dissolve a geodataframe
    on intersecting geometries instead of attributes. Use this function
    followed by the .dissolve(by='geometry_group') method. 

    Args:
        gdf (gpd.GeoDataFrame)  : input geodataframe with multi-geometries.
        buffer_value (float)    : buffer distance in crs units. Defaults
                                  to 0.0001.
        out_column_name (string): name of output column containing the group
                                  number. Defaults to 'geometry_group'

    Returns:
        gdf (gpd.GeoDataFrame) : geodataframe with new index column
                                 and grouped geometries.
    """
    gdf_polygon = gdf.copy()
    gdf_polygon['geometry'] = gdf_polygon.geometry.buffer(buffer_value,
                                                          resolution=1)
    gdf_polygon["group"] = 1
    gdf_polygon_dissolved = gdf_polygon.dissolve(by="group")
    gdf_out = explode(gdf_polygon_dissolved)
    gdf_out = gdf_out.reset_index()
    gdf_grouped = gpd.GeoDataFrame(gdf_out["level_1"],
                                   geometry=gdf_out.geometry)
    gdf_grouped.rename(columns={'level_1': out_column_name},
                       inplace=True)

    gdf_exploded = explode(gdf)
    gdf_exploded = gdf_exploded.reset_index()
    gdf_exploded_out = gpd.sjoin(gdf_exploded,
                                 gdf_grouped,
                                 how="left",
                                 op='within')
    return gdf_exploded_out


def stream_groups_per_basin():
    pass

    


# In[ ]:




# In[14]:

get_ipython().run_cell_magic('time', '', 'gdf_stream_groups = group_geometry(gdf_streams)')


# In[15]:

gdf_streams_simple = gpd.GeoDataFrame(gdf_stream_groups["geometry_group"],
                                      geometry=gdf_stream_groups.geometry)


# In[43]:

gdf_streams_simple["geometry_group_copy"] = gdf_streams_simple["geometry_group"]


# In[45]:

gdf_streams_grouped_simple = gdf_streams_simple.dissolve(by="geometry_group_copy")


# In[46]:

gdf_streams_grouped_simple


# In[47]:

get_ipython().run_cell_magic('time', '', 'gdf_test = gpd.sjoin(gdf_basins, gdf_streams_grouped_simple, how="left", op=\'intersects\')')


# In[48]:

gdf_test.columns


# In[49]:

gdf_test_simple = gpd.GeoDataFrame(gdf_test[["geometry_group","GDBD_ID"]],
                                   geometry=gdf_test.geometry)


# In[50]:

gdf_match = gdf_test_simple.loc[gdf_test["geometry_group"]>=0]


# In[51]:

gdf_match.groupby(['GDBD_ID']).agg(['mean', 'count'])


# In[52]:

gdf_deltas = gdf_match.loc[gdf_match["geometry_group"]>=0]


# In[53]:

gdf_deltas


# In[ ]:

gdf_test["stream_group"].unique()


# In[ ]:

a = gdf_test.loc[gdf_test['stream_group'] > 0]


# In[ ]:

a


# In[ ]:

output_path_shp = "{}gdf_streams_group_V{:02.0f}.shp".format(EC2_OUTPUT_PATH,OUTPUT_VERSION)
print(output_path_shp)


# In[ ]:

gdf_streams_group.to_file(output_path_shp,driver='ESRI Shapefile')


# In[ ]:

get_ipython().system('aws s3 cp --recursive {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH}')


# In[ ]:

test = gdf_basins.loc[7:9]


# In[ ]:

gdf_basins


# In[ ]:




# In[ ]:

gdf_test = gdf_out[1:100]


# In[ ]:

gdf_test.plot()


# In[ ]:




# In[ ]:

gdf_test.shape


# In[ ]:

tiny_value = 0.01
gdf_test_polygon = gdf_test.copy()
gdf_test_polygon['geometry'] = gdf_test_polygon.geometry.buffer(tiny_value)


# In[ ]:

gdf_test.head()


# In[ ]:

gdf_test["group"] = 1
gdf_test_dissolved = gdf_test.dissolve(by="group")
gdf_test_out = explode(gdf_test_dissolved)
gdf_test_out = gdf_test_out.reset_index()


# In[ ]:

gdf_test_out


# In[ ]:

gdf_test_out.plot(column="level_1")


# In[ ]:

gdf_test = gdf_test.reset_index()


# In[ ]:

gdf_test2 = gdf_test.copy()


# In[ ]:

gdf_test2.head()


# ## Tiny buffer

# In[ ]:




# In[ ]:




# In[ ]:

gdf_test2.head()


# In[ ]:

gdf_test2.plot()


# ## whatever

# In[ ]:

intersection = gpd.overlay(gdf_test2,gdf_test2, how='intersection')


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

gdf4 = gpd.sjoin(gdf_test, gdf_test2, how="inner", op='intersects')


# In[ ]:

gdf4.head()


# In[ ]:




# In[ ]:




# In[ ]:

shape = "streams"


# In[ ]:




# In[ ]:

print(output_path_shp)


# In[ ]:




# In[ ]:




# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



