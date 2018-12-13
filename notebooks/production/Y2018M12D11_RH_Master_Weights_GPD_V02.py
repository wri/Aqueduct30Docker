
# coding: utf-8

# In[1]:

""" Apply industry weights on merged table.
-------------------------------------------------------------------------------

This script applies the industry weights to the framework. Overall Water Risk
(OWR) is calculated for every industry. When scores are unavailable (nan),
the weights have been set to Nan to exclude them from the weight sum. 

Todo:
overall water risk scores per category (e.g.) have not been calculated yet. 


Author: Rutger Hofste
Date: 20181211
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = 'Y2018M12D11_RH_Master_Weights_GPD_V02'
OUTPUT_VERSION = 1

BQ_IN = {}
# Master Table
BQ_IN["MASTER"] = "y2018m12d04_rh_master_merge_rawdata_gpd_v02_v02"

# Weights
BQ_IN["WEIGHTS"] ="y2018m12d06_rh_process_weights_bq_v01_v01"

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

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

sql_master = """
SELECT
  string_id,
  pfaf_id,
  gid_0,
  gid_1,
  aqid,
  delta_id,
  indicator,
  raw,
  score,
  cat,
  label
FROM
  `{}.{}.{}`
ORDER BY
  string_id
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["MASTER"])


# In[7]:

df_master = pd.read_gbq(query=sql_master,dialect="standard")


# In[8]:

df_master.head()


# In[9]:

df_master.shape


# In[43]:

df_in = df_master


# In[44]:

sql_weights = """
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
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["WEIGHTS"])


# In[45]:

df_weights = pd.read_gbq(query=sql_weights,dialect="standard")


# In[46]:

df_weights.head()


# In[47]:

df_weights.shape


# In[48]:

df_groups = df_weights.loc[df_weights["industry_short"] =="DEF"][["indicator_short","group_short"]]


# In[49]:

df_groups = df_groups.apply(lambda x: x.astype(str).str.lower())


# In[50]:

df_groups


# In[ ]:




# In[ ]:




# In[51]:

# Add group to dataframe
df_in = pd.merge(left=df_in,
                 right=df_groups,
                 how="left",
                 left_on="indicator",
                 right_on="indicator_short")
df_in.drop("indicator_short",axis=1,inplace=True)


# In[56]:

df_industries = df_weights[["indicator_short","industry_short","weight_fraction"]]


# In[57]:

df_industries = df_industries.apply(lambda x: x.astype(str).str.lower())


# In[ ]:




# In[60]:

# Add industry to each indicator
df_w = pd.merge(left=df_in,
                right=df_industries,
                left_on = "indicator",
                right_on = "indicator_short",
                how = "right")


# In[67]:

# mask out weights where score is None
df_w["weight_fraction"] = df_w["weight_fraction"].mask(np.isnan(df_w["score"]))


# In[78]:

test = df_w.loc[(df_w["industry_short"] == "def") & (df_w["string_id"] == "111011-EGY.11_1-3365")]


# In[83]:

test.dtypes


# In[96]:

test2 = test["weight_fraction"]


# In[97]:

test2


# In[ ]:




# In[ ]:

industries = list(df_weights.industry_short.unique())
categories = list(df_weights.category_short.unique())
indicators = list(df_weights.indicator_short.unique())



# In[ ]:

# Calculate Overall Water Risks by using weights


# In[ ]:

df_merged_weights = df_master.copy()


# In[ ]:

for industry in industries:
    for indicator in indicators:
        column_name_weight = "{}_{}_weight".format(indicator.lower(),industry.lower())
        column_name_score = "{}_score".format(indicator.lower())
        column_name_weighted_score = "{}_{}_weightedscore".format(indicator.lower(),industry.lower())
        weight = df_weights.loc[(df_weights.industry_short == industry) & (df_weights.indicator_short == indicator)].weight_fraction.iloc[0]
        score = df_master[column_name_score]
               
        df_merged_weights[column_name_weight] = weight
        df_merged_weights[column_name_weighted_score] = score * weight


# In[ ]:

df_merged_weights.head()


# In[ ]:

def mask_weights():
    """
    Sets weights to np.nan when the score is np.nan
    
    this is required for later steps in which sums are 
    calculated.
        
    """

    for industry in industries:
        for indicator in indicators:
            weight_column_name = "{}_{}_weight".format(indicator.lower(),industry.lower())
            score_column_name = "{}_{}_weightedscore".format(indicator.lower(),industry.lower())
            df_merged_weights[weight_column_name] = df_merged_weights[weight_column_name].mask(np.isnan(df_merged_weights[score_column_name]))

    return 1

mask_weights()


# In[ ]:

df_master.head()


# In[ ]:

for industry in industries:    
    # weights    
    regex_w = '^.*_{}_weight$|^string_id$'.format(industry.lower())
    df_w = df_merged_weights.filter(regex=regex_w)
    df_w = df_w.set_index("string_id")
    column_name_w = "{}_weight_sum".format(industry.lower())
    df_merged_weights[column_name_w] = df_w.sum(axis=1).values
        
    # Overall scores
    regex_s = '^.*_{}_weightedscore$|^string_id$'.format(industry.lower())
    df_s = df_merged_weights.filter(regex=regex_s)
    df_s = df_s.set_index("string_id")
    column_name_s = "{}_weightedscore_sum".format(industry.lower())
    df_merged_weights[column_name_s] = df_s.sum(axis=1).values
    df_merged_weights["owr_{}_raw".format(industry.lower())] = df_merged_weights[column_name_s] / df_merged_weights[column_name_w]

    
    


# In[ ]:




# In[ ]:

df_merged_weights.head()


# In[ ]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[ ]:

df_merged_weights.to_gbq(destination_table=destination_table,
                         project_id=BQ_PROJECT_ID,
                         chunksize=10000,
                         if_exists="replace")


# In[ ]:

destination_path_s3 = "{}/{}.csv".format(ec2_output_path,SCRIPT_NAME)


# In[ ]:

df_merged_weights.to_csv(destination_path_s3)


# In[ ]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:



