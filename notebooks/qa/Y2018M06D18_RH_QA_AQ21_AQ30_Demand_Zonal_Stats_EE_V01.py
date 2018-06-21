
# coding: utf-8

# In[1]:

""" Zonal statistics with Aq21 basins as zones and AQ 2.1 and AQ 3.0 as values. 
-------------------------------------------------------------------------------
Zonal statistics for QA with the goal of comparing 


Author: Rutger Hofste
Date: 20180618
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:


"""

TESTING = 0
OVERWRITE_INPUT = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = "Y2018M06D18_RH_QA_AQ21_AQ30_Demand_Zonal_Stats_EE_V01"
OUTPUT_VERSION = 4

EXCLUDE_BASIN = 353020

INPUT_PATH_ZONES_AQ21 = "projects/WRI-Aquaduct/Y2018M06D11_RH_QA_Ingest_Aq21_Flux_Shapefile_V01/output_V03/aqueduct21_flux"
INPUT_PATH_ZONES_AQ21PROJ = "projects/WRI-Aquaduct/Y2018M06D19_RH_QA_Ingest_Aq21projection_Shapefile_V01/output_V01/aqueduct21projection_flux"
INPUT_PATH_ZONES_AQ30 = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_V04"
INPUT_PATH_VALUES_AQ21 = "projects/WRI-Aquaduct/Y2018M06D08_RH_QA_Aqueduct21_Demand_Ingest_GCS_EE_V01/output_V02/"

BUCKET = "aqueduct30_v01"

# Original Aqueduct 2.1 geodatabase crsTransform
CRS_TRANSFORM_AQ21 = [
    0.00833333333333333,
    0,
    -179.93746666666664,
    0,
    -0.00833333333333333,
    74.99583666666666
  ]

print("\Input_path_zones_aq21: " + INPUT_PATH_ZONES_AQ21 +
      "\Input_path_zones_aq21Projection: " + INPUT_PATH_ZONES_AQ21PROJ +
      "\nInput_path_zones_aq30: " + INPUT_PATH_ZONES_AQ30 +
      "\nInput_path_values_aq21: " + INPUT_PATH_VALUES_AQ21 + 
      "\nOutput_path_gcs: " + BUCKET)




# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import ee
ee.Initialize()


# In[4]:

def simplify_fc(f_in):
    """ Gets rid of unnecessary data before export
    
    Args:
        f_in (ee.Feature) : feature in. 
    
    """
    
    f_out = ee.Feature(None,{})    
    f_out = f_out.copyProperties(source=f_in,
                                  exclude=[])
    return ee.Feature(f_out)
    
    


# In[5]:

fc_aq21_zones = ee.FeatureCollection(INPUT_PATH_ZONES_AQ21)


# In[6]:

fc_aq21proj_zones = ee.FeatureCollection(INPUT_PATH_ZONES_AQ21PROJ)


# In[7]:

fc_aq30_zones_all = ee.FeatureCollection(INPUT_PATH_ZONES_AQ30)
fc_aq30_zones = fc_aq30_zones_all.filterMetadata("PFAF_ID","not_equals",EXCLUDE_BASIN)


# In[8]:

aqueduct_versions = ["aq21","aq30","aq21proj"]
sectors = ["a","d","i","t"]
demand_types = ["c","u"]
reducer = ee.Reducer.count().combine(ee.Reducer.sum(),"",True)

if TESTING:
    sectors = ["t"]
    demand_types = ["c","u"]
    aqueduct_versions = ["aq21","aq30","aq21proj"]
    


# In[9]:

for aqueduct_version in aqueduct_versions:    
    if aqueduct_version == "aq21":
        fc_zones = fc_aq21_zones
    elif aqueduct_version == "aq21proj":
        fc_zones = fc_aq21proj_zones
    elif aqueduct_version == "aq30":
        fc_zones = fc_aq30_zones
    else:
        break
    
    for demand_type in demand_types:
        for sector in sectors:
            print(aqueduct_version,demand_type,sector)
            input_file_name = demand_type + sector        
            i_demand = ee.Image(INPUT_PATH_VALUES_AQ21+input_file_name)
            fc_reduced = i_demand.reduceRegions(collection =fc_zones,
                                                reducer = reducer,
                                                crsTransform =CRS_TRANSFORM_AQ21
                                                )
            fc_reduced = ee.FeatureCollection(fc_reduced)
            
            fc_reduced_simplified = ee.FeatureCollection(fc_reduced.map(simplify_fc))

            file_name = "zonal_stats_{}_{}".format(input_file_name,aqueduct_version)
            file_name_prefix = "{}/output_V{:02.0f}/{}".format(SCRIPT_NAME,OUTPUT_VERSION,file_name)

            task = ee.batch.Export.table.toCloudStorage(collection=fc_reduced_simplified,
                                                        description = file_name,
                                                        bucket = BUCKET,
                                                        fileNamePrefix = file_name_prefix,
                                                        fileFormat = "CSV"
                                                        )

            task.start()
        


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous runs:  
# 0:00:40.154716  
# 0:00:42.719801
# 
# 

# In[ ]:



