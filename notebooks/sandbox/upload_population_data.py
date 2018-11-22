
# coding: utf-8

# In[7]:

GCS_INPUT_PATH = "gs://hackathon_pjs/population"


# In[11]:

SCRIPT_NAME = "upload_population_data"
OUTPUT_VERSION = 1

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)


# In[12]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')


# In[13]:

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[14]:

get_ipython().system('gsutil cp -r {GCS_INPUT_PATH}/*.zip {ec2_input_path}/.')


# In[20]:

get_ipython().system('unzip {ec2_input_path}/GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0.zip -d {ec2_input_path}')


# In[21]:

import os


# In[23]:

files = os.listdir(os.path.join(ec2_input_path,"GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0"))


# In[35]:

source = "{}/{}/{}".format(ec2_input_path,"GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0","GHS_POP_GPW42015_GLOBE_R2015A_54009_250_v1_0.tif")


# In[26]:

import rasterio
from rasterio.mask import mask
from shapely.geometry import MultiPolygon, Polygon


# In[27]:

minx = -63
maxx = -62
miny = 18
maxy = 18.5


# In[41]:

geom = Polygon([(minx,miny),
               (maxx,miny),
               (maxx,maxy),
               (minx,maxy)])


# In[29]:

geoms = MultiPolygon([geom])


# In[30]:

output_path = "{}/population.tif".format(ec2_output_path)


# In[36]:

source


# In[44]:

with rasterio.open(source) as src:
    meta = src.meta
    out_image, out_transform = mask(src, geoms,  crop=True) 
    #out_meta = src.meta.copy()
    #out_meta.update({"driver": "GTiff",
    #                 "height": out_image.shape[1],
    #                 "width": out_image.shape[2],
    #                 "transform": out_transform})

    """
    with rasterio.open(output_path, "w",**out_meta) as dest:
            dest.write(out_image)
    """


# In[45]:




# In[ ]:

get_ipython().system('gsutil cp {output_path} {GCS_INPUT_PATH}')


# In[ ]:



