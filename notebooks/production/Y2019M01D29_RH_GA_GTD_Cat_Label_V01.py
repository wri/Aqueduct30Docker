
# coding: utf-8

# In[1]:

""" Add category and label for groundwater stress and trend at state level.
-------------------------------------------------------------------------------

Add category and label to groundwater stress data. State level data. 
Aquifer level data was calculated in another script:
Y2018M09D03_RH_GWS_Cat_Label_V01

Groundwater Stress Categories:

<1 Low
1-5 Low - Medium
5-10 Medium - High
10-20 High
> 20 Extremely High

Linear interpolation groundwater stress

if x<1
    y = max(x,0)
elif 1 < x < 5
    y = (1/4)x + 3/4
elif 5 < x < 10
    y = 1/5 x + 1
elif 10 < x < 20 
    y = 1/10x + 2
elif x > 20
    y = min(x,5)


Groundwater Table Declining Trends Categories:
unit = cm/year

- 9999 NoData
- 9998 Insignificant trend
< 0 No Depletion
0 - 2 Low Depletion
2 - 8 Moderate Depletion
>8 High Depletion

however we need a 5 score category so. Names of categories TBD.

-1 -0 Low Depletion Risk -> No Depletion
0 - 2 Low-Medium Depletion Risk -> Moderate Depletion
2 - 4 Medium-High Depletion Risk - > Moderate Depletion
4 - 8 High Depletion Risk -> Moderate Depletion
>8 Extremely High Depletion Risk -> Extremely High Depletion

if x<0
    y = max(0,x+1)
elif 0 < x < 2
    y = (1/2)x + 1
elif 2 < x < 4
    y = (1/2) x + 1
elif 4 < x < 8 
    y = (1/4)x + 2
elif x > 8
    y = min((1/4)x + 2,5)


Author: Rutger Hofste
Date: 20190129
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name
    INPUT_VERSION (integer) : input version, see readme and output number
                              of previous script.
    OUTPUT_VERSION (integer) : output version for ec2 and s3.
    
    
Returns:

Result:
    Table on Google Bigquery.

"""

SCRIPT_NAME = "Y2019M01D29_RH_GA_GTD_Cat_Label_V01"
OUTPUT_VERSION = 1

BQ_PROJECT_ID = "aqueduct30"
BQ_OUTPUT_DATASET_NAME = "aqueduct30v01"
BQ_INPUT_TABLE_NAME = "y2018m09d03_rh_gws_tables_to_bq_v01_v01_state_table_sorted"
BQ_OUTPUT_TABLE_NAME = "{}_v{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION).lower()

print("BQ_OUTPUT_DATASET_NAME: ", BQ_OUTPUT_DATASET_NAME,
      "\nBQ_INPUT_TABLE_NAME: ",BQ_INPUT_TABLE_NAME,
      "\nBQ_OUTPUT_TABLE_NAME: ",BQ_OUTPUT_TABLE_NAME
      )


# In[ ]:



