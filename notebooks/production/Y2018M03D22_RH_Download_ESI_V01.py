
# coding: utf-8

# ### Download PcrGlobWB data to instance
# 
# * Purpose of script: Download ESI (Evaporative Stress Index) to Instance, Google Cloud Services and Amazon S3
# * Author: Rutger Hofste
# * Kernel used: python36
# * Date created: 20180322
# 
# * http://catalogue.servirglobal.net/Product?product_id=198
# 
# 
# I asked a question on stack exchange however the answer came too late.
# https://stackoverflow.com/questions/49448291/combining-wget-and-aws-s3-cp-to-upload-data-to-s3-without-saving-locally/49467225#49467225
# 
# You can maybe upgrade this with the --recursive flag
# 
# 

# In[1]:

import requests
import subprocess
import logging
import multiprocessing
import numpy as np
from lxml import html


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

URL = ["https://gis1.servirglobal.net/data/esi/"]
URL_BASE = "https://gis1.servirglobal.net"
url_child = "https://gis1.servirglobal.net/data/esi/12WK/2001/DFPPM_12WK_2001008.tif"

YEAR_MIN = 2001
YEAR_MAX = 2018

SCRIPT_NAME = "Y2018M03D22_RH_Download_ESI_V01"

EC2_OUTPUT_PATH = "/volumes/data/{}".format(SCRIPT_NAME)

OUTPUT_VERSION = 1

S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/{}/outputV{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)
GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}/outputV{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

TESTING = 0


# In[4]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/{}V{:02.0f}.log".format(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[5]:

get_ipython().system('rm -r {EC2_OUTPUT_PATH}')


# In[6]:

get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[7]:

get_ipython().system('gcloud config set project aqueduct30')


# In[8]:

def get_links(url):
    page = requests.get(url)
    tree = html.fromstring(page.content)
    webpage = html.fromstring(page.content)
    links = webpage.xpath('//a/@href')
    return links

def add_prefix(string):
    full_url = "{}{}".format(URL_BASE,string)
    return full_url

def download_file(url,ec2_output_path):
    """ Downloads a file to an EC2 instance
    
    Args:
        url (string) : full url to file
        ec2_output_path (string) : full path including extension to EC2.
                               Make sure the path exists
                               
    Returns:
        
    
    """
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        logger.debug("function download_file, downloaded" + url)
        with open(ec2_output_path, 'wb') as f:
            f.write(response.content)    
    else:
        logger.error("function download_file, failed" + url)
        

def copy_and_delete(ec2_path,s3_path,gcs_path,delete=False):
    """ Copies a file from EC2 to Amazon S3 and Google Cloud Storage
    
    Args:
        ec2_path (string) : full path to file on EC2.
        s3_path (string) : full path to output location on Amazon S3.
        gcs_path (string) : full path to output location on GCS.
        delete (boolean) : deletes the file on EC2. Defaults to False.
        
    Returns:
        result_s3 (string) : result of subprocess s3 upload command
        result_gcs (string) : result of subprocess gcs upload command
       
    """
    
    command_s3 = "aws s3 cp {} {}".format(ec2_path,s3_path)
    result_s3 = subprocess.check_output(command_s3,shell=True)
    command_gcs = "gsutil cp {} {}".format(ec2_path,gcs_path)
    result_gcs = subprocess.check_output(command_gcs,shell=True)
    
    if delete:
        command_ec2 = "rm {}".format(ec2_path) 
        result_ec2 = subprocess.check_output(command_ec2,shell=True)
    
    print(command_s3)
    print(command_gcs)
    
    logger.debug(command_s3)
    logger.debug(command_gcs)
    
    return result_s3, result_gcs
    
    
    
    


# In[9]:

result_level01 = []
for url in URL:
    level01_links = get_links(url)
    level01_links = list(map(add_prefix,level01_links[1:]))
    result_level01 = result_level01 + level01_links


# In[10]:

result_level02 = []
for url in result_level01:
    level02_links = get_links(url)
    level02_links = list(map(add_prefix,level02_links[1:]))
    result_level02 = result_level02 + level02_links


# In[11]:

result_level03 = []
for url in result_level02:
    level03_links = get_links(url)
    level03_links = list(map(add_prefix,level03_links[1:]))
    result_level03 = result_level03 + level03_links


# In[12]:

if TESTING:
    result_level03 = result_level03[1:15]


# In[13]:

len(result_level03)


# In[14]:

def url_to_cloud(url_array):
    """ uploads files to gcs and s3 using a temporary file on an EC2 instance.
    
    Args:
        array (np.array) : array with urls
    
    Returns:
        
    
    """
    
    for url in url_array:
        file_name = url.split("/")[-1]
        ec2_path = EC2_OUTPUT_PATH + "/" + file_name
        s3_path = S3_OUTPUT_PATH + "/" + file_name
        gcs_path = GCS_OUTPUT_PATH + "/" + file_name  
        download_file(url,ec2_path)
        copy_and_delete(ec2_path,s3_path,gcs_path,delete=True)
        end = datetime.datetime.now()
        elapsed = end - start
        print(elapsed)


# In[15]:

cpu_count = multiprocessing.cpu_count()
print(cpu_count)


# In[16]:

url_array = np.asarray(result_level03)


# In[17]:

url_arrays = np.array_split(url_array, cpu_count)


# In[18]:

get_ipython().run_cell_magic('time', '', '\np = multiprocessing.Pool()\nresults = p.map(url_to_cloud , url_arrays)\np.close()\np.join()')


# In[19]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

