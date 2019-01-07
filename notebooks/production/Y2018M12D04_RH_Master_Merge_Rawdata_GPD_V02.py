
# coding: utf-8

# In[1]:

""" Merge raw data into master table. 
-------------------------------------------------------------------------------

version 1 of this script concatenated the results horizontally whereas 
a vertical approach was more useful when applying the weights. 

version 2 of the script creates a vertical output. 

Author: Rutger Hofste
Date: 20181204
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = 'Y2018M12D04_RH_Master_Merge_Rawdata_GPD_V02'
OUTPUT_VERSION = 5

BQ_PROJECT_ID = "aqueduct30"
BQ_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


BQ_IN = {}
BQ_IN["Master"] = "y2018m12d06_rh_master_shape_v01_v01"


# GADM countries 
BQ_IN["GADM36L01"] = "y2018m11d12_rh_gadm36_level1_rds_to_bq_v01_v01"

INDICATORS = {"bws":"pfaf_id",
              "bwd":"pfaf_id",
              "iav":"pfaf_id",
              "sev":"pfaf_id",
              "cfr":"pfaf_id",
              "rfr":"pfaf_id",
              "drr":"pfaf_id",
              "gtd":"AqID_spatial_unit",
              "ucw":"adm0_a3",
              "cep":"pfaf_id",
              "udw":"pfaf_id",
              "usa":"pfaf_id",
              "rri":"adm0_a3"}

IDENTIFIERS = {"AqID_spatial_unit":"aqid",
               "adm0_a3":"gid_0",
               "pfaf_id":"pfaf_id"}


# Physical Risk Quantity | QAN   -------------

# Baseline Water Stress | BWS
BQ_IN["bws"] = "y2018m07d30_rh_gcs_to_bq_v01_v06"

# Baseline Water Depletion | BWD
BQ_IN["bwd"] = "y2018m07d30_rh_gcs_to_bq_v01_v06"

# Interannual Variability | IAV
BQ_IN["iav"] = "y2018m07d31_rh_inter_av_cat_label_v01_v02"

# Seasonal Variability | SEV
BQ_IN["sev"] = "y2018m08d02_rh_intra_annual_variability_cat_label_v01_v02"

# Riverine Flood Risk | RFR
BQ_IN["rfr"] = "y2018m12d04_rh_rfr_cfr_bq_v01_v01"

# Coastal Flood Risk | CFR
BQ_IN["cfr"] = "y2018m12d04_rh_rfr_cfr_bq_v01_v01"

# Drought Risk | DRR
BQ_IN["drr"] = "y2018m09d28_rh_dr_cat_label_v01_v02"

# Groundwater Table Decline | GTD
BQ_IN["gtd"] = "y2018m09d03_rh_gws_cat_label_v01_v01"


# Physical Risk Quality | QAL -----------------

# Untreated Collected Wastewater | UCW
BQ_IN["ucw"] = "y2018m12d04_rh_ucw_bq_v01_v02"

# Coastal Eutrophication Potential | CEP
BQ_IN["cep"] = "y2018m11d22_rh_icep_hybas6_cat_label_bq_v01_v03"

# Regulatory and Reputational Risk | RRR --------------

# Unimproved/no drinking water | UDW
BQ_IN["udw"] = "y2018m12d05_rh_udw_bq_v01_v01"

# Unimproved/no sanitation | USA
BQ_IN["usa"] = "y2018m12d05_rh_usa_bq_v01_v01"

# RepRisk Index | RRI
BQ_IN["rri"] = "y2018m12d05_rh_rri_bq_v01_v01"


ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("\nBQ_DATASET_NAME: ", BQ_DATASET_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ", BQ_OUTPUT_TABLE_NAME,
      "\ns3_output_path: ", s3_output_path,
      "\nec2_output_path:" , ec2_output_path)


# In[2]:

BQ_IN


# In[3]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[4]:

get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[5]:

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)

pd.set_option('display.max_columns', 500)


# In[6]:

# Master Table


# In[7]:

sql_master = """
SELECT
  string_id,
  pfaf_id,
  gid_1,
  aqid
FROM
  `{}.{}.{}`
ORDER BY
  aq30_id
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN["Master"])


# In[8]:

df_master = pd.read_gbq(query=sql_master,dialect="standard")


# In[9]:

df_master.head()


# In[10]:

sql_gadm36l01 = """
SELECT
  gid_1,
  gid_0
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN["GADM36L01"])


# In[11]:

df_gadm36l01 = pd.read_gbq(query=sql_gadm36l01,dialect="standard")


# In[12]:

df_gadm36l01.head()


# In[13]:

def process_bws():
    sql = """
    SELECT
      pfafid_30spfaf06 as pfaf_id,
      temporal_resolution,
      delta_id,
      year,
      waterstress_raw_dimensionless_coalesced as raw,
      waterstress_score_dimensionless_coalesced as score,
      waterstress_category_dimensionless_coalesced as cat,
      waterstress_label_dimensionless_coalesced as label
    FROM
      `{}.{}.{}`
    WHERE
      temporal_resolution = 'year'
      AND year = 2014
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN["bws"])
    df = pd.read_gbq(query=sql,dialect="standard")
    # Setting arid and low water use score to 5
    df.score.loc[df.score == -1] = 5
    return df

def process_bwd():
    sql = """
    SELECT
      pfafid_30spfaf06 as pfaf_id,
      temporal_resolution,
      delta_id,
      year,
      waterdepletion_raw_dimensionless_coalesced as raw,
      waterdepletion_score_dimensionless_coalesced as score,
      waterdepletion_category_dimensionless_coalesced as cat,
      waterdepletion_label_dimensionless_coalesced as label
    FROM
      `{}.{}.{}`
    WHERE
      temporal_resolution = 'year'
      AND year = 2014
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN["bwd"])
    df = pd.read_gbq(query=sql,dialect="standard")
    # Setting arid and low water use score to 5
    df.score.loc[df.score == -1] = 5
    return  df


def process_iav():
    sql = """
    SELECT
      pfafid_30spfaf06 as pfaf_id,
      temporal_resolution,
      year,
      delta_id,
      iav_riverdischarge_m_coalesced as raw,
      iav_riverdischarge_score_coalesced as score,
      iav_riverdischarge_category_coalesced as cat,
      iav_riverdischarge_label_coalesced as label
    FROM
      `{}.{}.{}`
    WHERE
      temporal_resolution = 'year'
      AND year = 2014
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN["iav"])
    df = pd.read_gbq(query=sql,dialect="standard")
    return  df

def process_sev():
    sql = """
    SELECT
      pfafid_30spfaf06 as pfaf_id,
      sv_riverdischarge_m_coalesced as raw,
      sv_riverdischarge_score_coalesced as score,
      sv_riverdischarge_category_coalesced as cat,
      sv_label_dimensionless_coalesced as label
    FROM
      `{}.{}.{}`
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN["sev"])
    df = pd.read_gbq(query=sql,dialect="standard")
    return df

def process_drr():
    sql = """
    SELECT
      PFAF_ID as pfaf_id,
      droughtrisk_score as raw,
      droughtrisk_score as score,
      droughtrisk_cat as cat,
      droughtrisk_label as label
    FROM
      `{}.{}.{}`
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN["drr"])
    df = pd.read_gbq(query=sql,dialect="standard")
    return df

def process_gtd():
    sql = """
    SELECT
      AqID_spatial_unit as aqid,
      groundwatertabledecliningtrend_cmperyear as raw,
      groundwatertabledecliningtrend_score as score,
      groundwatertabledecliningtrend_cat as cat,
      groundwatertabledecliningtrend_label as label
    FROM
      `{}.{}.{}`
    """.format(BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN["gtd"])
    df = pd.read_gbq(query=sql,dialect="standard")
    return df

def process_standard(indicator,identifier):
    sql = """
    SELECT
      {} as {},
      {}_raw as raw,
      {}_score as score,
      {}_cat as cat,
      {}_label as label
    FROM
      `{}.{}.{}`
    """.format(identifier,IDENTIFIERS[identifier],indicator,indicator,indicator,indicator,BQ_PROJECT_ID,BQ_DATASET_NAME,BQ_IN[indicator])
    df = pd.read_gbq(query=sql,dialect="standard")
    return df


def load_dataframe(indicator,identifier):
    if indicator == "bws":
        df = process_bws()
    elif indicator == "bwd":
        df = process_bwd()
    elif indicator == "iav":
        df = process_iav()
    elif indicator == "sev":
        df = process_sev()
    elif indicator == "drr":
        df = process_drr()
    elif indicator == "gtd":
        df = process_gtd()
    else:
        df = process_standard(indicator,identifier)
    return df
        


# In[14]:

d_1_out = {}
for indicator, identifier in INDICATORS.items():
    print(indicator,identifier)
    df = load_dataframe(indicator,identifier)
    df["indicator"] = indicator
    d_2_out = {}
    d_2_out["df"] = df
    d_2_out["indicator"] = indicator
    d_2_out["identifier1"] = identifier
    d_2_out["identifier2"] = IDENTIFIERS[identifier]
    d_1_out[indicator] = d_2_out


# # Join tables

# In[15]:

df_master_merged = pd.merge(left=df_master,
                     right=df_gadm36l01,
                     how="left",
                     left_on="gid_1",
                     right_on="gid_1",
                     validate="many_to_one")


# In[ ]:




# In[16]:

df_merged = []
for indicator, dictje in d_1_out.items():
    identifier = dictje["identifier2"]
    df_in = dictje["df"]
    df_out = pd.merge(left=df_master_merged,
                      right=df_in,
                      how="left",
                      left_on= identifier,
                      right_on= identifier,
                      validate="many_to_one")   
    df_merged.append(df_out)

df_result = pd.concat(df_merged, axis=0)
df_result_nones = df_result.replace(to_replace=[-9999,-9998,-9999.0,-9998.0,"-9999","NoData"],value= np.nan)


# In[17]:

df_result_nones.head()


# In[18]:

df_result_nones.shape


# In[19]:

destination_path_s3 = "{}/{}.pkl".format(ec2_output_path,SCRIPT_NAME)
df_result_nones.to_pickle(destination_path_s3)
get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[20]:

destination_table = "{}.{}".format(BQ_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)
print(destination_table)


# In[21]:

df_result_nones.to_gbq(destination_table=destination_table,
                       project_id=BQ_PROJECT_ID,
                       chunksize=100000,
                       if_exists="replace")


# In[22]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:02:28.099761  
# 0:03:18.624484
# 

# In[ ]:



