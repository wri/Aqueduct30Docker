
# coding: utf-8

# In[6]:

""" Share Aqueduct results with external party in multiple formats. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190114
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

The Aqueduct framework consist of 13 indicators. Each indicator has an 
associated geometry. 

There are 3 distinct geometries:
hydrobasin level 6 (hydrological sub-basins),
gadm level 1 (provinces) and 
whymap (groundwater aquifers)

we created a "master"  geometry by taking the union of the 3 geometries. 

Indicator overview:
"indicator_name_short", "indicator_name_long", "geometry name", "identifier"
"bws", "baseline water stress", "hydrobasin level 6", "pfaf_id"
"bwd", "baseline water depletion", "hydrobasin level 6", "pfaf_id"
"iav", "interannual variability", "hydrobasin level 6", "pfaf_id"
"sev", "Seasonal Variability","hydrobasin level 6", "pfaf_id"
"gtd", "Groundwater Table Decline", "groundwater aquifer", "aqid"
"drr", "Drought Risk","hydrobasin level 6", "pfaf_id"
"rfr", "Riverine Flood Risk","hydrobasin level 6", "pfaf_id"
"cfr", "Coastal Flood Risk","hydrobasin level 6", "pfaf_id"
"ucw", "Untreated Collected Wastewater","country","gid_0"
"cep", "Coastal Eutrophication Potential","hydrobasin level 6", "pfaf_id"
"udw", "Unimproved/no drinking water","hydrobasin level 6", "pfaf_id"
"usa", "Unimproved/no sanitation","hydrobasin level 6", "pfaf_id"
"rri", "RepRisk Index,"country","gid_0" 

10 indicators need to be joined at hydrobasin level 6, 
1 at groundwater aquifer level and 
2 at country (GADM level 0)

Overall Water Risk

A weighted average is calculated for three groups:

Water Quantity,
Water Quality,
and Regulatory and Reputational

The indicators are grouped as follows:

Water Quantity
    Baseline water stress
    Baseline water depletion 
    Groundwater table decline 
    Interannual variability 
    Seasonal variability
    Drought risk
    Riverine flood risk 
    Coastal flood risk 

Water Quality
    Untreated collected wastewater
    Coastal eutrophication potential

Regulatory and Reputational Risk
    Unimproved/no drinking water 
    Unimproved/no sanitation
    RepRisk Index (RRI)

An Overall Water Risk Score is calculated by taking a weighted 
average of the three groups. 

The weights per indicator are calculated for 10 different industries:

Default
Agriculture
Food & Beverage
Chemicals
Electric Power
Semiconductor
Oil & Gas
Mining
Construction Materials
Textile

Monthly Files:
most of the 13 indicators are only available at an annual temporal resolution.
For baseline water stress, baseline water depletion and interannual variability,
we also have monthly data. Tables with these monthly values will be stored 
separately. 


Options for improvement:
- for the drought indicator include hazard, exposure and vulnerability layers.
- include gridded withdrawal per indicator.


Output files:
- Master shapefile.
- Annual results normalized
- Annual result pivoted

- Monthly results
- Indust
- FAO Basin Names.
- GADM Country and Province Names.



"""


SCRIPT_NAME = 'Y2019M01D14_RH_Aqueduct_Results_V01'
OUTPUT_VERSION = 1

# GBQ
BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"

BQ_INPUT_TABLE  = {}

# RDS
RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_MASTER_GEOM_TABLE = "y2018m12d06_rh_master_shape_v01_v02"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("RDS_MASTER_GEOM_TABLE: ", RDS_MASTER_GEOM_TABLE,
      "\ns3_output_path: ", s3_output_path)


# In[7]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[18]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}/master_geom')


# In[19]:

import os
import sqlalchemy
import geopandas as gpd


# In[10]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,DATABASE_ENDPOINT,DATABASE_NAME))
connection = engine.connect()


# # Master Geometry

# In[14]:

sql = """
SELECT
    aq30_id,
    string_id,
    pfaf_id,
    gid_1, 
    aqid,
    geom
FROM {}
""".format(RDS_MASTER_GEOM_TABLE)


# In[15]:

gdf =gpd.GeoDataFrame.from_postgis(sql,engine,geom_col='geom')


# In[25]:

destination_path_master = os.path.join(ec2_output_path,"master_geom","master_geom.gpkg")


# In[26]:

destination_path_master


# In[27]:

gdf = gdf[0:100]


# In[28]:

gdf.to_file(filename=destination_path_master,driver="GPKG",encoding ='utf-8')


# # Annual Results

# # Monthly Resuls

# In[ ]:




# # Industry Weights

# In[ ]:



