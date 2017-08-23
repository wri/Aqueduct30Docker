
# coding: utf-8

# ### Merge FAO shapefiles with basin names
# 
# * Purpose of script: Create a shapefile with the merged files of the FAO database with basin namesm
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170823

# Data URL's, as you can see there is an error with the North America file on FAO's server. The data was downloaded using DownThemAll and stored on S3 

# | Geography | URL |
# |:---|:---|
# |Southeast Asia | http://www.fao.org/geonetwork/srv/en/main.home?uuid=ee616dc4-3118-4d67-ba05-6e93dd3e962f |
# |Near East | http://www.fao.org/geonetwork/srv/en/main.home?uuid=7ae00a40-642b-4637-b1d3-ffacb13360db |
# |Australia & New Zealand | http://www.fao.org/geonetwork/srv/en/main.home?uuid=a1a0e9ee-5062-4950-a6b9-fdd2284b2607 |
# |Africa | http://www.fao.org/geonetwork/srv/en/main.home?uuid=e54e2014-d23b-402b-8e73-c827628d17f4 |
# |Europe | http://www.fao.org/geonetwork/srv/en/main.home?uuid=1849e279-67bd-4e6f-a789-9918925a11a1 |
# |South America | http://www.fao.org/geonetwork/srv/en/main.home?uuid=d47ba28e-31be-470d-81cf-ad3d5594fafd |
# |Central America | http://www.fao.org/geonetwork/srv/en/main.home?uuid=bc9139e6-ccc9-4ded-a0c4-93b91cb54dde |
# |North America | http://ref.data.fao.org/map?entryId=b06dc828-3166-461a-a17d-26f4dc9f9819 |

# In[ ]:

S3_RAW_INPUT_PATH
S3_INPUT_PATH


# In[ ]:

Copy to working file


# In[ ]:

get_ipython().system('aws s3 cp ')

