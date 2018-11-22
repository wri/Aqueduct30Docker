
# coding: utf-8

# In[ ]:

### Downloads and Uploads data from AWS Earth Program and uploads to GCS. Super inefficient but it works. 


# In[1]:

ec2_input_path = "/volumes/data/hackathon_for_good/input"


# In[2]:

ec2_output_path = "/volumes/data/hackathon_for_good/output"


# In[3]:

get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[ ]:




# In[4]:

url = "http://opendata.digitalglobe.com/hurricane-irma/vector-data/2017-09-26/tomnod20170926/digitalglobe_crowdsourcing_hurricane_irma_20170926.zip"


# In[5]:

get_ipython().system('wget -O {ec2_input_path}/tomnod.zip {url}')


# In[6]:

get_ipython().system('unzip -o /volumes/data/hackathon_for_good/input/tomnod.zip -d /volumes/data/hackathon_for_good/input/')


# In[7]:

import geopandas as gpd
import subprocess


# In[8]:

input_path = "/volumes/data/hackathon_for_good/input/digitalglobe_crowdsourcing_hurricane_irma_20170926/digitalglobe_crowdsourcing_hurricane_irma_20170926.geojson"


# In[9]:

gdf = gpd.GeoDataFrame.from_file(input_path)


# In[10]:

gdf.shape


# In[11]:

gdf.head()


# In[12]:

for index, row in gdf.iterrows():
    print(index)
    s3_input_url = row["chip_url"]
    output_file_name = "{}.jpg".format(row["id"],row["label"])
    command = "wget -O {}/{} {}".format(ec2_output_path,output_file_name,s3_input_url)
    subprocess.check_output(command,shell=True)


# In[ ]:



