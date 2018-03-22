
# coding: utf-8

# ### Download PcrGlobWB data to instance
# 
# * Purpose of script: Download ESI (Evaporative Stress Index) to Instance and Amazon S3
# * Author: Rutger Hofste
# * Kernel used: python36
# * Date created: 20180322
# 
# 

# In[ ]:

from bs4 import BeautifulSoup
import requests
from lxml import html
import pandas as pd


# In[ ]:

URL = ["https://gis1.servirglobal.net/data/esi/"]
URL_BASE = "https://gis1.servirglobal.net"
url_child = "https://gis1.servirglobal.net/data/esi/12WK/2001/DFPPM_12WK_2001008.tif"

YEAR_MIN = 2001
YEAR_MAX = 2018

SCRIPT_NAME = "Y2018M03D22_RH_Download_ESI_V01"

EC2_OUTPUT_PATH = "/volumes/data/{}".format(SCRIPT_NAME)

BATCH_SIZE = 10


# In[ ]:

get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[ ]:

def get_links(url):
    page = requests.get(url)
    tree = html.fromstring(page.content)
    webpage = html.fromstring(page.content)
    links = webpage.xpath('//a/@href')
    return links

def add_prefix(string):
    full_url = "{}{}".format(URL_BASE,string)
    return full_url


# In[ ]:

result_level01 = []
for url in URL:
    level01_links = get_links(url)
    level01_links = list(map(add_prefix,level01_links[1:]))
    result_level01 = result_level01 + level01_links


# In[ ]:

result_level02 = []
for url in result_level01:
    level02_links = get_links(url)
    level02_links = list(map(add_prefix,level02_links[1:]))
    result_level02 = result_level02 + level02_links


# In[ ]:

result_level03 = []
for url in result_level02:
    level03_links = get_links(url)
    level03_links = list(map(add_prefix,level03_links[1:]))
    result_level03 = result_level03 + level03_links


# In[ ]:

for url in result_level03:
    print(url)  
    file_name = url.split("/")[-1]
    print(file_name)
    output_path = EC2_OUTPUT_PATH + "/" + file_name
    print(output_path)
    #response = requests.get(url, stream=True)
    #if response.status_code == 200:
        #with open(output_path, 'wb') as f:
            #f.write(response.content)


# In[ ]:



