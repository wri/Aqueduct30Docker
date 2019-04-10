
# coding: utf-8

# In[1]:

""" Apply industry weights on merged table.
-------------------------------------------------------------------------------

This script applies the industry weights to the framework. Overall Water Risk
(OWR) is calculated for every industry. When scores are unavailable (nan),
the weights have been set to Nan to exclude them from the weight sum. 

Grouped and overall water risks is calculated and stored as a separate 
indicator callend awr (aggregated water risk). 

Author: Rutger Hofste
Date: 20181211
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = 'Y2018M12D11_RH_Master_Weights_GPD_V02'
OUTPUT_VERSION = 10

BQ_IN = {}
# Master Table
BQ_IN["MASTER"] = "y2018m12d04_rh_master_merge_rawdata_gpd_v02_v09"

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
import scipy.interpolate
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "aqueduct30"
client = bigquery.Client(project=BQ_PROJECT_ID)

get_ipython().magic('matplotlib inline')
pd.set_option('display.max_columns', 500)


# In[6]:

sql_master = """
SELECT
  string_id,
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

df_master.shape


# In[9]:

df_in = df_master


# In[10]:

# certain GUs have invalid 'None' indicators. removing those
# This happens when the id exists in the master shapefile but not in te indicator results.

df_valid_in = df_in.loc[df_in["indicator"].notnull()]


# In[11]:

sql_weights = """
SELECT
  id,
  group_full,
  LOWER(group_short) AS group_short,
  indicator_full,
  LOWER(indicator_short) AS indicator_short,
  industry_full,
  LOWER(industry_short) AS industry_short,
  weight_abs,
  weight_label,
  weight_interpretation,
  weight_fraction
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["WEIGHTS"])


# In[12]:

df_weights = pd.read_gbq(query=sql_weights,dialect="standard")


# In[13]:

df_weights.head()


# In[14]:

df_weights.shape


# In[15]:

df_groups = df_weights.loc[df_weights["industry_short"] =="def"][["indicator_short","group_short"]]


# In[16]:

df_groups


# In[17]:

# Add group to dataframe
df_group_in = pd.merge(left=df_valid_in,
                 right=df_groups,
                 how="left",
                 left_on="indicator",
                 right_on="indicator_short")
df_group_in.drop("indicator_short",axis=1,inplace=True)


# In[18]:

df_group_in.tail()


# In[19]:

df_industries = df_weights[["indicator_short","industry_short","weight_fraction"]]


# In[20]:

# Add industry to each indicator
df_w = pd.merge(left=df_group_in,
                right=df_industries,
                left_on = "indicator",
                right_on = "indicator_short",
                how = "left")
df_w.drop("indicator_short",axis=1,inplace=True)


# In[21]:

# mask out weights where score is None
df_w["weight_fraction"] = df_w["weight_fraction"].mask(np.isnan(df_w["score"]))


# In[22]:

df_w["weighted_score"] = df_w["weight_fraction"] * df_w["score"]


# In[23]:

df_w.tail()


# In[24]:

def calculate_group_aggregate(df):
    """ Calculates the weighted scores for each industry, group pair. 
    e.g. Quantity risk for Agriculture (qan,agr).
    The dataframe will have an indicator called "awr" that stands for
    aggregated water risk. 
    
    
    Args:
        df (DataFrame) : Pandas Dataframe with Aqueduct values.
    
    Returns:
        df_agg (DataFrame) : DataFrame with aggregated scores. 
    
    """
    df_agg = df.groupby(["string_id","industry_short","group_short"])["weight_fraction","weighted_score"].agg("sum").reset_index()
    df_agg["indicator"] = "awr" # Aggregated Water Risk
    df_agg["raw"] = df_agg["weighted_score"] / df_agg["weight_fraction"]
    return df_agg

def calculate_total_aggregate(df_group):
    """ Calculates the weighted scores for each industry.
    e.g. Total risk for Agriculture
    The dataframe will have an indicator called "awr" that stands for
    aggregated water risk and a 'group' tot that stands for total.
    
    
    Args:
        df_group (DataFrame) : Pandas Dataframe with Grouped 
            Aqueduct values.
    
    Returns:
        df_totalagg (DataFrame) : DataFrame with aggregated scores. 
    
    """
    df_totalagg = df_group.groupby(["string_id","industry_short"])["weight_fraction","weighted_score"].agg("sum").reset_index()
    df_totalagg["group_short"] = "tot"
    df_totalagg["indicator"] = "awr" 
    df_totalagg["raw"] = df_totalagg["weighted_score"] / df_totalagg["weight_fraction"]
    return df_totalagg

def quantile_interp_function(s,q,y):
    """ Get a interpolated function based on quantiles.
    y and q should be the same length.
    
    Args:
        s(pandas Series): Input y data that needs to 
            be remapped.
        q(list): list with quantile x values.
        y(list): list with y value to map to.
        
    Returns:
        f(interp1d) : Scipy function object.
        quantiles(Pandas Series): list of quantile y 
            values. 
        
    Example:
    
        s = df["col"]
        q = [0,0.2,0.4,0.6,0.8,1]
        y = [0,1,2,3,4,5]
        f = quantile_interp_function(s,quantiles,y)
        y_new = f(x)
    
    """
    quantiles = s.quantile(q=q)
    f = scipy.interpolate.interp1d(quantiles,y)
    return f, quantiles

def calculate_group_remapped_scores(df_group):
    """ remap scores based on quantiles and linear
    interpolation. 
    
    See other functions for more information.
    
    Quantiles are determined per-group. 
    
    
    """
    
    groups = ["qan","qal","rrr"]
    q = [0,0.2,0.4,0.6,0.8,1]
    y = [0,1,2,3,4,5]

    ss_out = pd.Series() 
    for group in groups:
        s = df_group.loc[df_group["group_short"] == group]["raw"]
        f, quantiles = quantile_interp_function(s,q,y)
        print("quantile values used for group: ",group, "\n", quantiles)
        s_out  = df_group.loc[df_group["group_short"] == group]["raw"].apply(f)
        ss_out = ss_out.append(s_out)

    df_group["score"] = ss_out
    return df_group

def calculate_remapped_scores(df):
    q = [0,0.2,0.4,0.6,0.8,1]
    y = [0,1,2,3,4,5]
    s = df.loc[df["group_short"] == "tot"]["raw"]
    f, quantiles = quantile_interp_function(s,q,y)
    print("quantiles used for aggregate total:",quantiles)
    df["score"] = df["raw"].apply(f)
    return df

def score_to_category(score):
    if np.isnan(score):
        cat = np.nan
    elif score != 5:
        cat = int(np.floor(score))
    else:
        cat = 4
    return cat
    

def category_to_label(cat):
    if np.isnan(cat):
        label = "NoData"
    elif cat == 0:
        label = "Low (0-1)"
    elif cat == 1:
        label = "Low - Medium (1-2)"
    elif cat == 2:
        label = "Medium - High (2-3)"
    elif cat == 3:
        label = "High (3-4)"
    elif cat == 4: 
        label = "Extremely High (4-5)"
    else:
        label = "Error"
    return label


# In[25]:

df_group = calculate_group_aggregate(df_w)


# In[26]:

df_group = calculate_group_remapped_scores(df_group)


# In[27]:

df_total = calculate_total_aggregate(df_group)


# In[28]:

df_total = calculate_remapped_scores(df_total)


# In[29]:

df_agg = pd.concat([df_group, df_total],axis=0)


# In[30]:

df_agg["cat"] = df_agg["score"].apply(score_to_category)
df_agg["label"] = df_agg["cat"].apply(category_to_label)


# In[31]:

df_agg_out = pd.concat([df_w,df_agg],axis=0)


# In[32]:

df_agg_out.sort_index(axis=1,inplace=True)


# In[33]:

df_agg_out.head()


# In[34]:

df_agg_out.tail()


# In[35]:

destination_path_csv = "{}/{}.csv".format(ec2_output_path,SCRIPT_NAME)


# In[36]:

df_agg_out.to_csv(destination_path_csv)


# In[37]:

destination_path_pkl = "{}/{}.pkl".format(ec2_output_path,SCRIPT_NAME)


# In[38]:

df_agg_out.to_pickle(destination_path_pkl)


# In[39]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[40]:

destination_table = "{}.{}".format(BQ_OUTPUT_DATASET_NAME,BQ_OUTPUT_TABLE_NAME)


# In[41]:

# This can be sped up by using csv files, storing to GCS and ingesting from there.
df_agg_out.to_gbq(destination_table=destination_table,
                         project_id=BQ_PROJECT_ID,
                         chunksize=100000,
                         if_exists="replace")


# In[42]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:   
# 0:28:11.269342  
# 0:22:35.716177   
# 0:21:53.347870  
# 0:24:52.610386
# 

# In[ ]:



