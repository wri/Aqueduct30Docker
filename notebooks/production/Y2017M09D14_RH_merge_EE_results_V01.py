
# coding: utf-8

# # Calculate average PCRGlobWB supply using EE
# 
# * Purpose of script: This script will join the csv tables from GCS into one file using pandas
# * Author: Rutger Hofste
# * Kernel used: python35
# * Date created: 20170914

# In[1]:

import time
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
print(dateString,timeString)


# In[2]:

GCS_INPUT_PATH = "gs://aqueduct30_v01/Y2017M09D11_RH_zonal_stats_EE_V15/"
EC2_INPUT_PATH = "/volumes/data/Y2017M09D14_RH_merge_EE_results_V01/input"
EC2_OUTPUT_PATH = "/volumes/data/Y2017M09D14_RH_merge_EE_results_V01/output"
S3_OUTPUT_PATH = "s3://wri-projects/Aqueduct30/processData/Y2017M09D14_RH_merge_EE_results_V01/output/"


STRING_TRIM = "V15ee_export.csv"
# e.g. IrrLinearWW_monthY2014M12V15ee_export.csv -> IrrLinearWW_monthY2014M12

#Aux files, do not change order i.e. zones, area, extra
AUXFILES = ["Hybas06",
            "area_30s_m2",
            "ones_30s"
           ]

DROP_COLUMNS = [".geo","system:index"]

VERSION = 12

OUTPUTFILENAME = "mergedZonalStatsEE_V%0.2d" %(VERSION)


# In[3]:

get_ipython().system('rm -r {EC2_INPUT_PATH}')
get_ipython().system('rm -r {EC2_OUTPUT_PATH}')


# In[4]:

get_ipython().system('mkdir -p {EC2_INPUT_PATH}')
get_ipython().system('mkdir -p {EC2_OUTPUT_PATH}')


# In[5]:

get_ipython().system('gsutil cp -r {GCS_INPUT_PATH} {EC2_INPUT_PATH} ')


# In[6]:

import pandas as pd
import os
import re


# In[7]:

def createRegex(aList):
    return '|'.join(aList)

def prepareFile(oneFile):
        trimFileName = oneFile[:-len(STRING_TRIM)]
        d ={}
        d["df"] = pd.read_csv(os.path.join(folder,oneFile))
        d["df"] = prepareDf(d["df"])
        d["trimFileName"] = trimFileName
        return d         
        

def prepareDf(df):
    for column in df.columns:
        if re.search("PfafID",column):
            df2 = df.set_index(column)
            df2 = df2.drop(DROP_COLUMNS,1)        
            return df2
        


    
    


# In[8]:

folder = os.path.join(EC2_INPUT_PATH,"Y2017M09D11_RH_zonal_stats_EE_V15/")


# In[9]:

files = os.listdir(folder)


# ## Process Auxiliary Datasets (PfafID, Area, Ones)

# In[10]:

dAux ={}
for regex in AUXFILES:
    r = re.compile(regex)
    newList = filter(r.match, files)
    oneFile = list(newList)[0]
    dAux[regex] = prepareFile(oneFile)   


# In[11]:

regex = createRegex(AUXFILES)


# In[12]:

print(regex)


# In[13]:

d ={}
dAux ={}
for oneFile in files: 
    trimFileName = oneFile[:-len(STRING_TRIM)]    
    if not re.search(regex,oneFile):
        d[trimFileName] = prepareFile(oneFile)
        
    elif re.search(regex,oneFile):
        dAux[trimFileName] = prepareFile(oneFile)
    
    else:
        print("Unrecognized file name, check STRING_TRIM variable")
        


# In[14]:

dfLeft = dAux[AUXFILES[0]]["df"]


# # Adding area to shapes

# In[15]:

dAux[AUXFILES[1]]["df"]["total_%s" %(AUXFILES[1])] = dAux[AUXFILES[1]]["df"]["count_%s" %(AUXFILES[1])] * dAux[AUXFILES[1]]["df"]["mean_%s" %(AUXFILES[1])]


# In[16]:

dfMerge = dAux[AUXFILES[0]]["df"].merge(dAux[AUXFILES[1]]["df"],
                       how="outer",
                       left_index=True,
                       right_index=True,
                       sort=True
                      )


# In[17]:

for key, value in d.items():
    dfNew = value["df"].copy()
    # total new value = area in m^2 times mean flux 
    dfNew["total_volume_%s" %(value["trimFileName"])] = dAux[AUXFILES[1]]["df"]["total_%s" %(AUXFILES[1])] * value["df"]["mean_%s" %(value["trimFileName"])]
    
     
    
    dfMerge = dfMerge.merge(dfNew,
                           how="outer",
                           left_index=True,
                           right_index=True,
                           sort=True                   
                           )


# In[18]:

dfMerge.head()


# In[19]:

dfMerge.to_csv(os.path.join(EC2_OUTPUT_PATH,OUTPUTFILENAME+".csv"))


# In[20]:

dfMerge.to_pickle(os.path.join(EC2_OUTPUT_PATH,OUTPUTFILENAME+".pkl"))


# In[21]:

outputLocation = os.path.join(S3_OUTPUT_PATH,OUTPUTFILENAME)


# In[22]:

get_ipython().system('aws s3 cp --recursive {EC2_OUTPUT_PATH} {S3_OUTPUT_PATH}')


# In[ ]:



