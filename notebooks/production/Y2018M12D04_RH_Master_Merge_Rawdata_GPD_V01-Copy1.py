
# coding: utf-8

# In[1]:

""" Merge raw data into master table. 
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181204
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = 'Y2018M12D04_RH_Master_Merge_Rawdata_GPD_V01'
OUTPUT_VERSION = 2

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()


BQ_IN = {}


BQ_IN["Master"] = "y2018m12d06_rh_master_shape_v01_v01"


# Additional tables required

# area per smallest subunit
BQ_IN["area"] = "y2018m12d07_rh_process_area_bq_v01_v01"

# GADM countries 
BQ_IN["GADM36L01"] = "y2018m11d12_rh_gadm36_level1_rds_to_bq_v01_v01"


# FAO basins



# Area per aq30_id

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
BQ_IN["CEP"] = "y2018m11d22_rh_icep_hybas6_cat_label_bq_v01_v03"

# Regulatory and Reputational Risk | RRR --------------

# Unimproved/no drinking water | UDW
BQ_IN["UDW"] = "y2018m12d05_rh_udw_bq_v01_v01"

# Unimproved/no sanitation | USA
BQ_IN["USA"] = "y2018m12d05_rh_usa_bq_v01_v01"

# RepRisk Index | RRI
BQ_IN["RRI"] = "y2018m12d05_rh_rri_bq_v01_v01"


ec2_output_path = "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
s3_output_path = "s3://wri-projects/Aqueduct30/processData/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)



print("\nBQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
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
  aq30_id,
  string_id,
  pfaf_id,
  gid_1,
  aqid
FROM
  `{}.{}.{}`
ORDER BY
  aq30_id
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["Master"])


# In[8]:

df_master = pd.read_gbq(query=sql_master,dialect="standard")


# In[9]:

df_master.head()


# In[10]:

sql_area = """
SELECT
  string_id,
  area_km2
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["area"])


# In[11]:

df_area = pd.read_gbq(query=sql_area,dialect="standard")


# In[12]:

df_area.head()


# In[13]:

df_area.shape


# In[14]:

# GADM Cross Table


# In[15]:

sql_gadm36l01 = """
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
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["GADM36L01"])


# In[16]:

df_gadm36l01 = pd.read_gbq(query=sql_gadm36l01,dialect="standard")


# In[17]:

df_gadm36l01.head()


# In[18]:

# BWS annual


# In[19]:

sql_bws_annual = """
SELECT
  pfafid_30spfaf06,
  temporal_resolution,
  delta_id,
  year,
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


# In[20]:

df_bws_annual = pd.read_gbq(query=sql_bws_annual,dialect="standard")


# In[21]:

df_bws_annual = df_bws_annual.rename(columns={"pfafid_30spfaf06":"pfaf_id",
                                              "waterstress_raw_dimensionless_coalesced":"bws_raw",
                                              "waterstress_score_dimensionless_coalesced":"bws_score",
                                              "waterstress_category_dimensionless_coalesced":"bws_cat",
                                              "waterstress_label_dimensionless_coalesced":"bws_label"})


# In[22]:

df_bws_annual.head()


# In[23]:

# Setting arid and low water use score to 5
df_bws_annual.bws_score.loc[df_bws_annual.bws_score == -1] = 5


# In[24]:

# BWD annual


# In[25]:

sql_bwd_annual = """
SELECT
  pfafid_30spfaf06,
  temporal_resolution,
  delta_id,
  year,
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


# In[26]:

df_bwd_annual = pd.read_gbq(query=sql_bwd_annual,dialect="standard")


# In[27]:

df_bwd_annual = df_bwd_annual.rename(columns={"pfafid_30spfaf06":"pfaf_id",
                                              "waterdepletion_raw_dimensionless_coalesced":"bwd_raw",
                                              "waterdepletion_score_dimensionless_coalesced":"bwd_score",
                                              "waterdepletion_category_dimensionless_coalesced":"bwd_cat",
                                              "waterdepletion_label_dimensionless_coalesced":"bwd_label"})


# In[28]:

df_bwd_annual.drop(columns=["temporal_resolution",
                            "year",
                            "delta_id"],
                   inplace=True)


# In[29]:

# Setting arid and low water use score to 5
df_bwd_annual.bwd_score.loc[df_bwd_annual.bwd_score == -1] = 5


# In[30]:

# IAV 


# In[31]:

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


# In[32]:

df_iav_annual = pd.read_gbq(query=sql_iav_annual,dialect="standard")


# In[33]:

df_iav_annual = df_iav_annual.rename(columns={"pfafid_30spfaf06":"pfaf_id",
                                              "iav_riverdischarge_m_coalesced":"iav_raw",
                                              "iav_riverdischarge_score_coalesced":"iav_score",
                                              "iav_riverdischarge_category_coalesced":"iav_cat",
                                              "iav_riverdischarge_label_coalesced":"iav_label"})


# In[34]:

df_iav_annual.drop(columns=["temporal_resolution",
                            "year",
                            "delta_id"],
                   inplace=True)


# In[35]:

# SEV


# In[36]:

sql_sev = """
SELECT
  pfafid_30spfaf06,
  sv_riverdischarge_m_coalesced,
  sv_riverdischarge_score_coalesced,
  sv_riverdischarge_category_coalesced,
  sv_label_dimensionless_coalesced
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["SEV"])


# In[37]:

df_sev = pd.read_gbq(query=sql_sev,dialect="standard")


# In[38]:

df_sev = df_sev.rename(columns={"pfafid_30spfaf06":"pfaf_id",
                                "sv_riverdischarge_m_coalesced":"sev_raw",
                                "sv_riverdischarge_score_coalesced":"sev_score",
                                "sv_riverdischarge_category_coalesced":"sev_cat",
                                "sv_label_dimensionless_coalesced":"sev_label"})


# In[39]:

# RFR


# In[40]:

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


# In[41]:

df_rfr = pd.read_gbq(query=sql_rfr,dialect="standard")


# In[42]:

# CFR


# In[43]:

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


# In[44]:

df_cfr = pd.read_gbq(query=sql_cfr,dialect="standard")


# In[45]:

# DRR


# In[46]:

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


# In[47]:

df_drr = pd.read_gbq(query=sql_drr,dialect="standard")


# In[48]:

df_drr = df_drr.rename(columns={"PFAF_ID":"pfaf_id",
                                "droughtrisk_raw":"drr_raw",
                                "droughtrisk_score":"drr_score",
                                "droughtrisk_cat":"drr_cat",
                                "droughtrisk_label":"drr_label"})


# In[49]:

# GTD


# In[50]:

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


# In[51]:

df_gtd = pd.read_gbq(query=sql_gtd,dialect="standard")


# In[52]:

df_gtd = df_gtd.rename(columns={"AqID_spatial_unit":"aqid",
                                "groundwatertabledecliningtrend_cmperyear":"gtd_raw",
                                "groundwatertabledecliningtrend_score":"gtd_score",
                                "groundwatertabledecliningtrend_cat":"gtd_cat",
                                "groundwatertabledecliningtrend_label":"gtd_label"})


# In[53]:

# UCW


# In[54]:

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


# In[55]:

df_ucw = pd.read_gbq(query=sql_ucw,dialect="standard")


# In[56]:

# CEP


# In[57]:

sql_cep = """
SELECT
  pfaf_id,
  cep_raw,
  cep_score,
  cep_cat,
  cep_label
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["CEP"])


# In[58]:

df_cep = pd.read_gbq(query=sql_cep,dialect="standard")


# In[59]:

# UDW


# In[60]:

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


# In[61]:

df_udw = pd.read_gbq(query=sql_udw,dialect="standard")


# In[62]:

# USA


# In[63]:

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


# In[64]:

df_usa = pd.read_gbq(query=sql_usa,dialect="standard")


# In[65]:

# RRI


# In[66]:

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


# In[67]:

df_rri = pd.read_gbq(query=sql_rri,dialect="standard")


# # Joining  Tables

# In[68]:

df_master.head()


# In[69]:

df_master.shape


# In[70]:

# Merge Area


# In[71]:

df_merged = pd.merge(left=df_master,
                     right=df_area,
                     how="left",
                     left_on="string_id",
                     right_on="string_id",
                     validate="one_to_one")


# In[72]:

# Merge GADM


# In[73]:

df_gadm36l01.head()


# In[74]:

df_merged = pd.merge(left=df_merged,
                     right=df_gadm36l01,
                     how="left",
                     left_on="gid_1",
                     right_on="gid_1",
                     validate="many_to_one")


# In[75]:

# Merge BWS


# In[76]:

df_merged = pd.merge(left=df_merged,
                     right=df_bws_annual,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[77]:

# Merge BWD


# In[78]:

df_merged = pd.merge(left=df_merged,
                     right=df_bwd_annual,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[79]:

# Merge IAV


# In[80]:

df_merged = pd.merge(left=df_merged,
                     right=df_iav_annual,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[81]:

# Merge SEV


# In[82]:

df_merged = pd.merge(left=df_merged,
                     right=df_sev,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[83]:

# Merge RFR


# In[84]:

df_merged = pd.merge(left=df_merged,
                     right=df_rfr,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[85]:

# Merge CFR


# In[86]:

df_merged = pd.merge(left=df_merged,
                     right=df_cfr,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[87]:

# Merge DRR


# In[88]:

df_merged = pd.merge(left=df_merged,
                     right=df_drr,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[89]:

# Merge GTD


# In[90]:

df_merged = pd.merge(left=df_merged,
                     right=df_gtd,
                     how="left",
                     left_on="aqid",
                     right_on="aqid",
                     validate="many_to_one")


# In[91]:

# UCW


# In[92]:

df_merged = pd.merge(left=df_merged,
                     right=df_ucw,
                     how="left",
                     left_on="gid_0",
                     right_on="adm0_a3",
                     validate="many_to_one")


# In[93]:

df_merged.drop(columns=["adm0_a3"],
               inplace=True)


# In[94]:

df_merged.head()


# In[95]:

# CEP


# In[96]:

df_merged = pd.merge(left=df_merged,
                     right=df_cep,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[97]:

# UDW


# In[98]:

df_merged = pd.merge(left=df_merged,
                     right=df_udw,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[99]:

# USA


# In[100]:

df_merged = pd.merge(left=df_merged,
                     right=df_usa,
                     how="left",
                     left_on="pfaf_id",
                     right_on="pfaf_id",
                     validate="many_to_one")


# In[101]:

df_merged.head()


# In[102]:

# RRI


# In[103]:

df_merged = pd.merge(left=df_merged,
                     right=df_rri,
                     how="left",
                     left_on="gid_0",
                     right_on="adm0_a3",
                     validate="many_to_one")


# In[104]:

df_merged.drop(columns=["adm0_a3"],
               inplace=True)


# In[105]:

# The dataframe has None's and NoData Values. 
# Creating two versions. One with None's and one with NoData values. 


# In[106]:

df_merged_nones = df_merged.replace(to_replace=[-9999,-9998,-9999.0,-9998.0,"-9999","NoData"],value= np.nan)


# In[107]:

df_merged_nones.head()


# In[108]:

destination_path_s3 = "{}/{}.pkl".format(ec2_output_path,SCRIPT_NAME)


# In[109]:

df_merged_nones.to_pickle(destination_path_s3)


# In[110]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[111]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[112]:

destination_table


# In[113]:

df_merged_nones.to_gbq(destination_table=destination_table,
                       project_id=BQ_PROJECT_ID,
                       chunksize=10000,
                       if_exists="replace")


# In[114]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:02:28.099761
# 

# In[ ]:



