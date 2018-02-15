
# coding: utf-8

# ### Y2018M02D15_RH_GDBD_Merge_V01
# 
# * Purpose of script: This script will reproject GDBD basins and streams and merge them. 
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20180215
# 
# 
# Basins and Streams extracted from GDBD databases downloaded from:
# 
# http://www.cger.nies.go.jp/db/gdbd/data/Africa.zip
# http://www.cger.nies.go.jp/db/gdbd/data/Asia.zip
# http://www.cger.nies.go.jp/db/gdbd/data/Europe.zip
# http://www.cger.nies.go.jp/db/gdbd/data/Oceania.zip
# http://www.cger.nies.go.jp/db/gdbd/data/N_America.zip
# http://www.cger.nies.go.jp/db/gdbd/data/S_America.zip
# 
# Unzipped, streams and basins opened with ArcGIS Desktop 10.5 and exported to shapefiles. 
# 
# Shapefile of Asia caused problems with reprojection. Use Reproject in ArcMap. 
# 
# 

# In[1]:

SCRIPT_NAME = "Y2018M02D15_RH_GDBD_Merge_V01"

EC2_INPUT_PATH  = ("/volumes/data/{}/input/").format(SCRIPT_NAME)
EC2_OUTPUT_PATH = ("/volumes/data/{}/output/").format(SCRIPT_NAME)

S3_INPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2018M02D15_RH_GDBD_Basins_Streams_SHP_V01"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/{}/output".format(SCRIPT_NAME)


# In[2]:

print(EC2_OUTPUT_PATH)


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH} {EC2_INPUT_PATH} --recursive')


# In[42]:

import subprocess
import numpy as np
import pandas as pd
import geopandas as gpd
import getpass
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon

get_ipython().magic('matplotlib inline')


# In[6]:

target_crs = {'init': 'epsg:4326'}


# In[30]:

def explode(gdf):
    indf = gdf
    outdf = gpd.GeoDataFrame(columns=indf.columns)
    for idx, row in indf.iterrows():
        if type(row.geometry) == Polygon:
            outdf = outdf.append(row,ignore_index=True)
        if type(row.geometry) == MultiPolygon:
            multdf = gpd.GeoDataFrame(columns=indf.columns)
            recs = len(row.geometry)
            multdf = multdf.append([row]*recs,ignore_index=True)
            for geom in range(recs):
                multdf.loc[geom,'geometry'] = row.geometry[geom]
            outdf = outdf.append(multdf,ignore_index=True)
    return outdf


# In[7]:

shapes = ["basins","streams"]
continents = ["af","as","eu","na","oc","sa"]

df = pd.DataFrame()

for shape in shapes:
    for continent in continents:
        dictje = {}
        dictje["path"] = "{}{}/{}_{}.shp".format(EC2_INPUT_PATH,shape,continent,shape)
        print(dictje["path"])
        dictje["shape"] = shape
        dictje["continent"] = continent
        df = df.append(pd.Series(dictje),ignore_index=True)
    
        


# In[11]:

result_dict = {}

for shape in shapes:
    df_selection = df.loc[df['shape'] == shape]
    gdf_out = gpd.GeoDataFrame()
    for index, row in df_selection.iterrows():
        gdf = gpd.read_file(row["path"])
        gdf2 = gdf.to_crs(target_crs)
        gdf_out = gdf_out.append(gdf2)
                          
    
    output_path = "{}GDBD_{}_EPSG4326_V01.shp".format(EC2_OUTPUT_PATH,shape)
    print(output_path)
    gdf_out.to_file(output_path,driver='ESRI Shapefile')
    result_dict[shape] = gdf_out
    


# In[12]:

gdf_test = gpd.read_file("/volumes/data/Y2018M02D15_RH_GDBD_Merge_V01/input/basins/af_basins.shp",driver='ESRI Shapefile')


# In[23]:

gdf_test


# In[28]:

gdf_test.head()


# In[31]:

gdf_test2 = explode(gdf_test)


# In[35]:

gdf_test.shape


# In[34]:

gdf_test2.shape


# In[36]:

b = gdf_test.explode()


# In[38]:

gdf_test= gdf_test.set_index("GDBD_ID")


# In[39]:




# In[ ]:

gs_test = explode(gdf_test)


# In[ ]:

exploded = df.explode().reset_index().rename(columns={0: 'geometry'})
merged = exploded.merge(df.drop('geometry', axis=1), left_on='level_0', right_index=True)


# In[ ]:




# In[ ]:




# In[ ]:




# In[107]:

gdf_test = gpd.read_file("/volumes/data/Y2018M02D15_RH_GDBD_Merge_V01/input/streams/af_streams.shp",driver='ESRI Shapefile')


# In[108]:

gdf_test["GDBD_ID"] = gdf_test['GDBD_ID'].apply(lambda x: np.int64(x))


# In[109]:




# In[110]:

gdf_test = gdf_test.set_index("GDBD_ID")


# In[111]:

exploded = gdf_test.explode()


# In[120]:

exploded= exploded.reset_index().rename(columns={0: 'geometry'})


# In[121]:

exploded = exploded.to_frame()


# In[ ]:

exploded = exploded.set_index("GDBD_ID")


# In[92]:

merged = exploded.merge(gdf_test.drop('geometry', axis=1), left_on='level_0', right_index=True)


# In[93]:

merged = merged.set_index(['level_0', 'level_1']).set_geometry('geometry')


# In[94]:

merged.head()


# In[25]:

type(b)


# In[27]:

len(b)


# In[21]:

a = gdf_test.loc[100].geometry


# In[22]:

type(a)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

get_ipython().system('aws s3 cp {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH} --recursive ')


# In[ ]:




# In[ ]:




# In[ ]:

fileLocation = "{}/basins/eu_basins.shp".format(EC2_INPUT_PATH )


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

gdf = gpd.read_file(fileLocation)


# In[ ]:

gdf.plot()


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

password = getpass.getpass()


# In[ ]:

gis = GIS("https://www.arcgis.com","rhofste_worldresources",password)


# Website http://www.cger.nies.go.jp/db/gdbd/gdbd_index_e.html accessed on Feb 15 2018

# In[ ]:

continents = ["Africa","Asia","Europe","Oceania","N_America","S_America"]


# In[ ]:

URLs = []

for continent in continents:
    URLs = URLs + ["http://www.cger.nies.go.jp/db/gdbd/data/{}.zip".format(continent)]
    



# In[ ]:

df["continent"] = df.index


# In[ ]:

for index, row in df.iterrows():
    command = "wget -O {}{}.zip {}".format(EC2_INPUT_PATH,index,row["URL"])
    print(command)
    subprocess.check_output(command,shell=True)


# Make sure folder is empty

# In[ ]:

for index, row in df.iterrows():
    command = "unzip {}{}.zip -d {}".format(EC2_INPUT_PATH,index,EC2_INPUT_PATH)
    print(command)
    response = subprocess.check_output(command,shell=True)
    print(response)
    


# In[ ]:

properties =


# In[ ]:

df["properties"] = df.apply(lambda row:  {
                    'title':"GDBD {}".format(row["continent"]),
                    'tags':'GDBD',
                    'type':'Shapefile'}
                            , axis=1) 


# In[ ]:

for index, row in df.iterrows():
    print(row["URL"])


# In[ ]:

test_properties = {'title':'Parks and Open Space',
                'tags':'parks, open data, devlabs',
                'type':'Shapefile'}


# In[ ]:




# In[ ]:




# In[ ]:



