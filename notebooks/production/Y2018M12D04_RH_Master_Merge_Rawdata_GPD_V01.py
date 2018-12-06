
# coding: utf-8

# In[73]:

""" Merge raw data into master table. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181204
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = 'Y2018M12D04_RH_Master_Merge_Rawdata_GPD_V0'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


BQ_IN = {}

# Physical Risk Quantity | QAN   -------------

# Baseline Water Stress | BWS
BQ_IN["BWS"] = "y2018m07d30_rh_gcs_to_bq_v01_v06"

# Baseline Water Depletion | BWD
BQ_IN["BWD"] = "y2018m07d30_rh_gcs_to_bq_v01_v06"

# Interannual Variability | IAV
BQ_IN["IAV"] = "y2018m07d31_rh_inter_av_cat_label_v01_v02"

# Seasonal Variability | SEV
BQ_IN["SEV"] = "y2018m08d02_rh_intra_annual_variability_cat_label_v01_v02"

# Riverine Flood Risk | RFR
BQ_IN["RFR"] = "y2018m12d04_rh_rfr_cfr_bq_v01_v01"

# Coastal Flood Risk | CFR
BQ_IN["CFR"] = "y2018m12d04_rh_rfr_cfr_bq_v01_v01"

# Drought Risk | DRR
BQ_IN["DRR"] = "y2018m09d28_rh_dr_cat_label_v01_v02"

# Groundwater Table Decline | GTD
BQ_IN["GTD"] = "y2018m09d03_rh_gws_cat_label_v01_v01"


# Physical Risk Quality | QAL -----------------

# Untreated Collected Wastewater | UCW
BQ_IN["UCW"] = "y2018m12d04_rh_ucw_bq_v01_v01"

# Coastal Eutrophication Potential | CEP
BQ_IN["CEP"] = "y2018m11d22_rh_icep_hybas6_cat_label_bq_v01_v02"

# Regulatory and Reputational Risk | RRR --------------

# Unimproved/no drinking water | UDW
BQ_IN["UDW"] = "y2018m12d05_rh_udw_bq_v01_v01"

# Unimproved/no sanitation | USA
BQ_IN["USA"] = "y2018m12d05_rh_usa_bq_v01_v01"

# RepRisk Index | RRI
BQ_IN["RRI"] = "y2018m12d05_rh_rri_bq_v01_v01"



print("\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ", BQ_OUTPUT_TABLE_NAME)


# In[7]:

BQ_IN


# In[8]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[9]:

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)


# In[ ]:

# BWS annual


# In[23]:

sql_bws_annual = """
SELECT
  pfafid_30spfaf06,
  temporal_resolution,
  delta_id,
  year,
  month,
  waterstress_raw_dimensionless_coalesced,
  waterstress_score_dimensionless_coalesced,
  waterstress_category_dimensionless_coalesced,
  waterstress_label_dimensionless_coalesced
FROM
  `{}.{}.{}`
WHERE
  temporal_resolution = 'year'
  AND year = 2014
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["BWS"])


# In[24]:

df_bws_annual = pd.read_gbq(query=sql_bws_annual,dialect="standard")


# In[27]:

df_bws_annual = df_bws_annual.rename(columns={"pfafid_30spfaf06":"pfafid",
                                              "waterstress_raw_dimensionless_coalesced":"bws_raw",
                                              "waterstress_score_dimensionless_coalesced":"bws_score",
                                              "waterstress_category_dimensionless_coalesced":"bws_cat",
                                              "waterstress_label_dimensionless_coalesced":"bws_label"})


# In[ ]:

# BWD annual


# In[37]:

sql_bwd_annual = """
SELECT
  pfafid_30spfaf06,
  temporal_resolution,
  delta_id,
  year,
  month,
  waterdepletion_raw_dimensionless_coalesced,
  waterdepletion_score_dimensionless_coalesced,
  waterdepletion_category_dimensionless_coalesced,
  waterdepletion_label_dimensionless_coalesced
FROM
  `{}.{}.{}`
WHERE
  temporal_resolution = 'year'
  AND year = 2014
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["BWD"])


# In[38]:

df_bwd_annual = pd.read_gbq(query=sql_bwd_annual,dialect="standard")


# In[39]:

df_bwd_annual = df_bwd_annual.rename(columns={"pfafid_30spfaf06":"pfafid",
                                              "waterdepletion_raw_dimensionless_coalesced":"bwd_raw",
                                              "waterdepletion_score_dimensionless_coalesced":"bwd_score",
                                              "waterdepletion_category_dimensionless_coalesced":"bwd_cat",
                                              "waterdepletion_label_dimensionless_coalesced":"bwd_label"})


# In[40]:

# IAV 


# In[41]:

sql_iav_annual = """
SELECT
  pfafid_30spfaf06,
  temporal_resolution,
  year,
  delta_id,
  iav_riverdischarge_m_coalesced,
  iav_riverdischarge_score_coalesced,
  iav_riverdischarge_category_coalesced,
  iav_riverdischarge_label_coalesced
FROM
  `{}.{}.{}`
WHERE
  temporal_resolution = 'year'
  AND year = 2014
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["IAV"])


# In[42]:

df_iav_annual = pd.read_gbq(query=sql_iav_annual,dialect="standard")


# In[43]:

df_iav_annual = df_iav_annual.rename(columns={"pfafid_30spfaf06":"pfafid",
                                              "iav_riverdischarge_m_coalesced":"iav_raw",
                                              "iav_riverdischarge_score_coalesced":"iav_score",
                                              "iav_riverdischarge_category_coalesced":"iav_cat",
                                              "iav_riverdischarge_label_coalesced":"iav_label"})


# In[45]:

# SEV


# In[46]:

sql_sev = """
SELECT
  pfafid_30spfaf06,
  delta_id,
  sv_riverdischarge_m_coalesced,
  sv_riverdischarge_score_coalesced,
  sv_riverdischarge_category_coalesced,
  sv_label_dimensionless_coalesced
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["SEV"])


# In[48]:

df_sev = pd.read_gbq(query=sql_sev,dialect="standard")


# In[49]:

df_sev = df_sev.rename(columns={"pfafid_30spfaf06":"pfafid",
                                "sv_riverdischarge_m_coalesced":"sev_raw",
                                "sv_riverdischarge_score_coalesced":"sev_score",
                                "sv_riverdischarge_category_coalesced":"sev_cat",
                                "sv_label_dimensionless_coalesced":"sev_label"})


# In[50]:

# RFR


# In[51]:

sql_rfr = """
SELECT
  pfaf_id,
  rfr_raw,
  rfr_score,
  rfr_cat,
  rfr_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["RFR"])


# In[52]:

df_rfr = pd.read_gbq(query=sql_rfr,dialect="standard")


# In[53]:

# CFR


# In[54]:

sql_cfr = """
SELECT
  pfaf_id,
  cfr_raw,
  cfr_score,
  cfr_cat,
  cfr_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["CFR"])


# In[55]:

df_cfr = pd.read_gbq(query=sql_cfr,dialect="standard")


# In[56]:

# DRR


# In[57]:

sql_drr = """
SELECT
  PFAF_ID,
  droughtrisk_score AS droughtrisk_raw,
  droughtrisk_score,
  droughtrisk_cat,
  droughtrisk_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["DRR"])


# In[58]:

df_drr = pd.read_gbq(query=sql_drr,dialect="standard")


# In[59]:

df_drr = df_drr.rename(columns={"PFAF_ID":"pfafid",
                                "droughtrisk_raw":"drr_raw",
                                "droughtrisk_score":"drr_score",
                                "droughtrisk_cat":"drr_cat",
                                "droughtrisk_label":"drr_label"})


# In[60]:

# GTD


# In[61]:

sql_gtd = """
SELECT
  AqID_spatial_unit,
  groundwatertabledecliningtrend_cmperyear,
  groundwatertabledecliningtrend_score,
  groundwatertabledecliningtrend_cat,
  groundwatertabledecliningtrend_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["GTD"])


# In[63]:

df_gtd = pd.read_gbq(query=sql_gtd,dialect="standard")


# In[64]:

df_gtd = df_gtd.rename(columns={"AqID_spatial_unit":"aqid",
                                "groundwatertabledecliningtrend_cmperyear":"gtd_raw",
                                "roundwatertabledecliningtrend_score":"gtd_score",
                                "groundwatertabledecliningtrend_cat":"gtd_cat",
                                "groundwatertabledecliningtrend_label":"gtd_label"})


# In[65]:

# UCW


# In[67]:

sql_ucw = """
SELECT
  adm0_a3,
  ucw_raw,
  ucw_score,
  ucw_cat,
  ucw_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["UCW"])


# In[68]:

df_ucw = pd.read_gbq(query=sql_ucw,dialect="standard")


# In[69]:

# CEP


# In[74]:

sql_cep = """
SELECT
  pfaf_id,
  ice_raw,
  ice_score,
  ice_cat,
  ice_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["CEP"])


# In[75]:

df_cep = pd.read_gbq(query=sql_cep,dialect="standard")


# In[77]:

# UDW


# In[78]:

sql_udw = """
SELECT
  pfaf_id,
  udw_raw,
  udw_score,
  udw_cat,
  udw_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["UDW"])


# In[79]:

df_udw = pd.read_gbq(query=sql_udw,dialect="standard")


# In[80]:

# USA


# In[81]:

sql_usa = """
SELECT
  pfaf_id,
  usa_raw,
  usa_score,
  usa_cat,
  usa_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["USA"])


# In[82]:

df_usa = pd.read_gbq(query=sql_usa,dialect="standard")


# In[83]:

# RRI


# In[84]:

sql_rri = """
SELECT
  adm0_a3,
  rri_raw,
  rri_score,
  rri_cat,
  rri_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["RRI"])


# In[85]:

df_rri = pd.read_gbq(query=sql_rri,dialect="standard")


# In[ ]:

# approaches, join without geometry or with geometry

