
# coding: utf-8

# In[1]:

""" Compare country aggregations, create charts and combined database.
-------------------------------------------------------------------------------

Author: Rutger Hofste
Date: 20190606
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

SCRIPT_NAME = "Y2019M06D06_RH_AQ30VS21_Country_Comparison_V01"
OUTPUT_VERSION = 1

S3_INPUT_PATH_AQ30 = "s3://wri-projects/Aqueduct30/finalData/Y2019M04D15_RH_GA_Aqueduct_Results_V01/output_V03"
S3_INPUT_PATH_AQ21 = "s3://wri-projects/Aqueduct30/processData/Y2019M06D06_RH_AQ21_Country_Rankings_Simple_V01/output_V01"

ec2_input_path = "/volumes/data/{}/input_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 
ec2_output_path =  "/volumes/data/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION) 

s3_output_path = "s3://wri-projects/Aqueduct30/Aq30vs21/{}/output_V{:02.0f}/".format(SCRIPT_NAME,OUTPUT_VERSION)

print("S3_INPUT_PATH_AQ30: " + S3_INPUT_PATH_AQ30,
      "S3_INPUT_PATH_AQ21: " + S3_INPUT_PATH_AQ21,
      "\nec2_input_path: " + ec2_input_path +
      "\nec2_output_path: " + ec2_output_path+ 
      "\ns3_output_path: " + s3_output_path)


# ## Pre-Processing
# 
# Before the country ranking of Aqueduct could be loaded as a .csv, a few edits were made. 
# 
# 1. Download the data in excel format
# 1. Copy sheet baseline water stress to new excel file
# 1. Delete column "hist"
# 1. Add column with ADM03 codes (taken from AQ30 data)
# 1. Add column new_name to match the GADM country names
# 1. Upload to S3://s3://wri-projects/Aqueduct30/processData/Y2019M06D06_RH_AQ21_Country_Rankings_Simple_V01/output_V01/
# 
# 

# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_AQ30} {ec2_input_path} --recursive')


# In[5]:

get_ipython().system('aws s3 cp {S3_INPUT_PATH_AQ21} {ec2_input_path} --recursive')


# In[6]:

import os
import pandas as pd


# In[7]:

os.listdir(ec2_input_path)


# In[8]:

aq21_input_filename = "Y2019M06D06_RH_AQ21_Country_Rankings_Simple_V01.csv"
aq30_input_filename = "Y2019M04D15_RH_GA_Aqueduct_Results_V01_country_V03.csv"


# In[9]:

df_aq21_og = pd.read_csv("{}/{}".format(ec2_input_path,aq21_input_filename))


# In[10]:

df_aq21_og.head()


# In[11]:

df_aq30_og = pd.read_csv("{}/{}".format(ec2_input_path,aq30_input_filename))


# In[12]:

df_aq30_og.head()


# In[13]:

def pre_process_aq21(df):
    df_sel = df[["Rank","aq30_gid_0","new_name","All sectors","Agricultural","Domestic" ,"Industrial"]]
    df_sel.rename(columns={"Rank":"aq21_rank",
                           "new_name":"country_name",
                           "All sectors":"bws_s_aq21_tot",
                           "Agricultural":"bws_s_aq21_agg",
                           "Domestic":"bws_s_aq21_dom",
                           "Industrial":"bws_s_aq21_ind"},
                 inplace=True)
    return df_sel

def pre_process_aq30(df):
    df_sel = df.loc[(df["indicator_name"]=="bws") & (df["weight"]=="Tot")]
    df_sel.rename(columns={"score":"bws_s_aq30_tot",
                           "score_ranked":"aq30_rank"},
                 inplace=True)
    
    df_sel = df_sel[["gid_0","name_0","bws_s_aq30_tot","aq30_rank","fraction_valid"]]
    return df_sel


# In[14]:

df_aq21 = pre_process_aq21(df_aq21_og)


# In[15]:

df_aq21.head()


# In[16]:

df_aq30 = pre_process_aq30(df_aq30_og)


# In[17]:

df_bws_aq30vs21 = df_aq21.merge(right=df_aq30,
                                how="inner",
                                left_on="aq30_gid_0",
                                right_on="gid_0")


# In[18]:

df_bws_aq30vs21.head()


# In[19]:

df_bws_aq30vs21 = df_bws_aq30vs21[["gid_0","name_0","bws_s_aq21_tot","bws_s_aq30_tot","aq21_rank","aq30_rank","fraction_valid"]]


# In[20]:

output_filename = "Y2019M06D06_RH_AQ30VS21_Country_Comparison_V01.csv"
output_path = "{}/{}".format(ec2_output_path,output_filename)


# In[21]:

df_bws_aq30vs21.to_csv(path_or_buf=output_path)


# In[22]:

get_ipython().system('aws s3 cp {ec2_output_path} {s3_output_path} --recursive')


# In[23]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# previous run:  
# 0:00:07.447482
# 
