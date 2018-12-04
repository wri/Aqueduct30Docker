
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
SCRIPT_NAME = 'Y2018M12D04_RH_Master_Merge_Rawdata_GPD_V0'
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"


# Physical Risk Quantity | QAN   -------------

# Baseline Water Stress | BWS
BQ_IN["BWS"] = "y2018m07d30_rh_gcs_to_bq_v01_v06"

# Baseline Water Depletion | BWD
BQ_IN["BWD"] = "y2018m07d30_rh_gcs_to_bq_v01_v06"

# Groundwater Table Decline | GTD
BQ_IN["GTD"] = "y2018m09d03_rh_gws_cat_label_v01_v01"

# Interannual Variability | IAV
BQ_IN["IAV"] = "y2018m07d31_rh_inter_av_cat_label_v01_v02"

# Seasonal Variability | SEV
BQ_IN["SEV"] = "y2018m08d02_rh_intra_annual_variability_cat_label_v01_v02"

# Drought Risk | DRR
BQ_IN["DRR"] = "y2018m09d28_rh_dr_cat_label_v01_v02"

# Riverine Flood Risk | RFR
BQ_IN["RFR"] = ""

# Coastal Flood Risk | CFR
BQ_IN["CFR"] = ""

# Physical Risk Quality | QAL -----------------

# Untreated Collected Wastewater | UCW
BQ_IN["UCW"] = ""

# Coastal Eutrophication Potential | CEP
BQ_IN["CEP"] = "y2018m11d22_rh_icep_hybas6_cat_label_bq_v01_v01"

# Regulatory and Reputational Risk | RRR --------------

# Unimproved/no drinking water | UDW
BQ_IN["UDW"] = ""

# Unimproved/no sanitation | USA
BQ_IN["USA"] = ""

# RepRisk Index | RRI
BQ_IN["RRI"] = ""



RDS_DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
RDS_DATABASE_NAME = "database01"


BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("\nRDS_DATABASE_ENDPOINT: ", RDS_DATABASE_ENDPOINT,
      "\nRDS_DATABASE_NAME: ", RDS_DATABASE_NAME,
      "\nRDS_INPUT_TABLE_NAME: ",RDS_INPUT_TABLE_NAME,
      "\nBQ_OUTPUT_DATASET_NAME_WKT: ", BQ_OUTPUT_DATASET_NAME_WKT,
      "\nBQ_OUTPUT_DATASET_NAME_GEOG: ", BQ_OUTPUT_DATASET_NAME_GEOG,
      "\nBQ_OUTPUT_TABLE_NAME: ", BQ_OUTPUT_TABLE_NAME)


# In[ ]:



