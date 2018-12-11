
# coding: utf-8

# In[1]:

""" Apply industry weights on merged table.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20181211
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""



# Weights
BQ_IN["weights"] ="y2018m12d06_rh_process_weights_bq_v01_v01"


# In[2]:

# Calculate Overall Water Risks by using weights


# In[3]:

df_weights.head()


# In[ ]:

sectors = list(df_weights.sector_short.unique())
categories = list(df_weights.category_short.unique())
indicators = list(df_weights.indicator_short.unique())


# In[ ]:

# Add weights to table


# In[ ]:

df_merged_weights = df_merged_nones


# In[ ]:

for sector in sectors:
    for indicator in indicators:
        column_name_weight = "weight_{}_{}".format(indicator.lower(),sector.lower())
        column_name_score = "{}_score".format(indicator.lower())
        column_name_weighted_score = "weight_{}_{}_score".format(indicator.lower(),sector.lower())
        weight = df_weights.loc[(df_weights.sector_short == sector) & (df_weights.indicator_short == indicator)].weight_fraction.iloc[0]
        score = df_merged_nones[column_name_score]             
        df_merged_weights[column_name_weight] = weight
        df_merged_weights[column_name_weighted_score] = score


# In[ ]:

df_merged_weights.head()


# In[ ]:

# Calculate Overall Water Risk Scores


# In[ ]:

df_scores_selection = df_merged_weights.filter(regex='^weight_.*_score$')


# In[ ]:

sql_weights = """
SELECT
  id,
  category_full_name,
  category_short,
  indicator_name_full,
  indicator_short,
  sector_full,
  sector_short,
  weight_abs,
  weight_label,
  weight_interpretation,
  weight_fraction
FROM
  `{}.{}.{}`
""".format(BQ_PROJECT_ID,BQ_OUTPUT_DATASET_NAME,BQ_IN["weights"])


# In[ ]:

df_weights = pd.read_gbq(query=sql_weights,dialect="standard")


# In[ ]:




# In[ ]:



