
# coding: utf-8

# In[1]:

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

10 indicators at hydrobasin level 6, 
1 at groundwater aquifer level and 
2 at country (GADM level 0)

the master shapefile has a unique identifier called string_id

the format of string_id is {pfaf_id} - {gid_1} - {aqid}
114415-SOM.7_1-3306

pfaf_id = 114415
gid_1 = SOM.7_1 (Somalia, province 7_1)
aqid = 3306

when files contain a string_id, you should use that to join the data. 

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
- Master shapefile
- Annual results normalized
- Annual result pivoted

- Monthly results
- Industry weights
- FAO Basin Names
- GADM Country and Province Names


"""


SCRIPT_NAME = 'Y2019M01D14_RH_Aqueduct_Results_V01'
OUTPUT_VERSION = 2

# GBQ
BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"

BQ_INPUT_TABLE  = {}
BQ_INPUT_TABLE["annual_normalized"] = "y2018m12d11_rh_master_weights_gpd_v02_v06"
BQ_INPUT_TABLE["annual_pivot"] = "y2018m12d14_rh_master_horizontal_gpd_v01_v06"
BQ_INPUT_TABLE["monthly_normalized_bws"] = "y2018m07d30_rh_gcs_to_bq_v01_v06"
BQ_INPUT_TABLE["monthly_normalized_bwd"] = "y2018m07d30_rh_gcs_to_bq_v01_v06"
BQ_INPUT_TABLE["monthly_normalized_iav"] = "y2018m07d31_rh_inter_av_cat_label_v01_v02"
BQ_INPUT_TABLE["industry_weights"] = "y2018m12d06_rh_process_weights_bq_v01_v01"
BQ_INPUT_TABLE["gadm"] = "y2018m11d12_rh_gadm36_level1_rds_to_bq_v01_v01"

S3_INPUT_PATH_ANNUAL_NORMALIZED = "s3://wri-projects/Aqueduct30/processData/Y2018M12D11_RH_Master_Weights_GPD_V02/output_V06"

# RDS
RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"
RDS_MASTER_GEOM_TABLE = "y2018m12d06_rh_master_shape_v01_v02"

RDS_FAO_LINK = "fao_link_v07"
RDS_FAO_MINOR = "fao_minor_v07"
RDS_FAO_MAJOR = "fao_major_v07"



ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/finalData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("RDS_MASTER_GEOM_TABLE: ", RDS_MASTER_GEOM_TABLE,
      "\nS3_INPUT_PATH_ANNUAL_NORMALIZED: ", S3_INPUT_PATH_ANNUAL_NORMALIZED,
      "\ns3_output_path: ", s3_output_path)


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}/master_geom')
get_ipython().system('mkdir -p {ec2_output_path}/annual')
get_ipython().system('mkdir -p {ec2_output_path}/monthly')
get_ipython().system('mkdir -p {ec2_output_path}/industry_weights')
get_ipython().system('mkdir -p {ec2_output_path}/fao')
get_ipython().system('mkdir -p {ec2_output_path}/gadm')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_ANNUAL_NORMALIZED} {ec2_input_path} --recursive')


# In[5]:

import os
import sqlalchemy
import pandas as pd
import geopandas as gpd
from google.cloud import bigquery


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[6]:

F = open("/.password","r")
password = F.read().splitlines()[0]
F.close()

engine = sqlalchemy.create_engine("postgresql://rutgerhofste:{}@{}:5432/{}".format(password,RDS_DATABASE_ENDPOINT,RDS_DATABASE_NAME))
connection = engine.connect()


# In[7]:

total_out = {}


# # Master Geometry

# In[8]:

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

data_out ={}
data_out["data"] =gpd.GeoDataFrame.from_postgis(sql,engine,geom_col='geom')
data_out["path"] = os.path.join(ec2_output_path,"master_geom","master_geom")
total_out["master"] = data_out


# # Annual results normalized

# In[9]:

source_path_annual_normalized = "{}/Y2018M12D11_RH_Master_Weights_GPD_V02.pkl".format(ec2_input_path)


# In[10]:

data_out ={}
data_out["data"] = pd.read_pickle(source_path_annual_normalized)
data_out["path"] = destination_path_annual_normalized = os.path.join(ec2_output_path,"annual","annual_normalized")
total_out["annual_normalized"] = data_out


# # Annual result pivoted

# In[11]:

data_out ={}
sql_annual_pivot = """
SELECT
  * EXCEPT (geom)
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE["annual_pivot"])
data_out["data"] = pd.read_gbq(query=sql_annual_pivot,
                              dialect="standard")
data_out["path"] = os.path.join(ec2_output_path,"annual","annual_pivot")
total_out["annual_pivot"] = data_out


# # Monthly Results

# ## Monthly Results | BWS

# In[12]:

def process_bws():
    sql = """
    SELECT
      pfafid_30spfaf06 as pfaf_id,
      temporal_resolution,
      year,
      month,
      delta_id,
      waterstress_raw_dimensionless_coalesced as raw,
      waterstress_score_dimensionless_coalesced as score,
      waterstress_category_dimensionless_coalesced as cat,
      waterstress_label_dimensionless_coalesced as label
    FROM
      `{}.{}.{}`
    WHERE
      temporal_resolution  = 'month'
      AND year = 2014
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE["monthly_normalized_bws"])
    df = pd.read_gbq(query=sql,dialect="standard")
    # Setting arid and low water use score to 5
    df.score.loc[df.score == -1] = 5
    return df


# In[13]:

data_out ={}
data_out["data"] = process_bws()
data_out["path"] = os.path.join(ec2_output_path,"monthly","monthly_bws")
total_out["monthly_bws"] = data_out


# ## Monthly Results  |  BWD

# In[14]:

def process_bwd():
    sql = """
    SELECT
      pfafid_30spfaf06 as pfaf_id,
      temporal_resolution,
      year,
      month,
      delta_id,
      waterdepletion_raw_dimensionless_coalesced as raw,
      waterdepletion_score_dimensionless_coalesced as score,
      waterdepletion_category_dimensionless_coalesced as cat,
      waterdepletion_label_dimensionless_coalesced as label
    FROM
      `{}.{}.{}`
    WHERE
      temporal_resolution  = 'month'
      AND year = 2014
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE["monthly_normalized_bwd"])
    df = pd.read_gbq(query=sql,dialect="standard")
    # Setting arid and low water use score to 5
    df.score.loc[df.score == -1] = 5
    return  df


# In[15]:

data_out ={}
data_out["data"] = process_bwd()
data_out["path"] = os.path.join(ec2_output_path,"monthly","monthly_bwd")
total_out["monthly_bwd"] = data_out


# ## Monthly Results | IAV

# In[16]:

def process_iav():
    sql = """
    SELECT
      pfafid_30spfaf06 as pfaf_id,
      temporal_resolution,
      year,
      month,
      delta_id,
      iav_riverdischarge_m_coalesced as raw,
      iav_riverdischarge_score_coalesced as score,
      iav_riverdischarge_category_coalesced as cat,
      iav_riverdischarge_label_coalesced as label
    FROM
      `{}.{}.{}`
    WHERE
      temporal_resolution = 'month'
      AND year = 2014
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE["monthly_normalized_iav"])
    df = pd.read_gbq(query=sql,dialect="standard")
    return  df


# In[17]:

data_out ={}
data_out["data"] = process_iav()
data_out["path"] = os.path.join(ec2_output_path,"monthly","monthly_iav")
total_out["monthly_iav"] = data_out


# #  FAO Basin Names

# ## FAO Link

# In[18]:

sql_fao_link = """
SELECT
    pfaf_id,
    fao_id
FROM {}
""".format(RDS_FAO_LINK)


# In[19]:

data_out ={}
data_out["data"] = pd.read_sql_query(sql=sql_fao_link,
                                     con=connection)
data_out["path"] = os.path.join(ec2_output_path,"fao","fao_link")
total_out["fao_link"] = data_out




# ## FAO Minor

# In[20]:

sql_fao_minor = """
SELECT
    fao_id,
    sub_bas,
    to_bas,
    maj_bas,
    sub_name,
    sub_area
FROM {}
""".format(RDS_FAO_MINOR)


# In[21]:

data_out ={}
data_out["data"] = pd.read_sql_query(sql=sql_fao_minor,
                                     con=connection)
data_out["path"] = os.path.join(ec2_output_path,"fao","fao_minor")
total_out["fao_minor"] = data_out


# ## FAO Major

# In[22]:

sql_fao_major = """
SELECT
    maj_bas,
    maj_name,
    maj_area
FROM {}
""".format(RDS_FAO_MAJOR)


# In[23]:

data_out ={}
data_out["data"] = pd.read_sql_query(sql=sql_fao_major,
                                     con=connection)
data_out["path"] = os.path.join(ec2_output_path,"fao","fao_major")
total_out["fao_major"] = data_out


# # Industry Weights

# In[24]:

sql_industry_weights = """
SELECT
  id,
  group_full,
  group_short,
  indicator_full,
  indicator_short,
  industry_full,
  industry_short,
  weight_abs,
  weight_label,
  weight_interpretation,
  weight_fraction
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE["industry_weights"])


# In[25]:

data_out ={}
data_out["data"] = pd.read_gbq(query=sql_industry_weights,
                                dialect= "standard")
data_out["path"] = os.path.join(ec2_output_path,"industry_weights","industry_weights")
total_out["industry_weights"] = data_out


# # GADM Country and Province Names

# In[26]:

sql_gadm = """
SELECT
  gid_1,
  name_1,
  gid_0,
  name_0,
  varname_1,
  nl_name_1,
  type_1,
  engtype_1,
  cc_1,
  hasc_1
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_INPUT_TABLE["gadm"])


# In[27]:

data_out ={}
data_out["data"] = df_gadm = pd.read_gbq(query=sql_gadm,
                                         dialect="standard")
data_out["path"] = os.path.join(ec2_output_path,"gadm","gadm")
total_out["gadm"] = data_out


# ## Export

# In[ ]:

for key, data_out in total_out.items():
    print("writing ", key, " to: ", data_out["path"])
    
    if key == "master":
        data_out["data"].to_file(filename=data_out["path"]+".gpkg",
                                 driver="GPKG",
                                 encoding ='utf-8')
    else:
        data_out["data"].to_pickle(path=data_out["path"]+".pkl")
        data_out["data"].to_csv(path_or_buf=data_out["path"]+".pkl")
    


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 
