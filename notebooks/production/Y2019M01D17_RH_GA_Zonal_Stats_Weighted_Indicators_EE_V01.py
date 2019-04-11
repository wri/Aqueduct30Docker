
# coding: utf-8

# In[1]:

"""Zonal statistics for GADM level 1 for sum of weights and weights*indicators.
-------------------------------------------------------------------------------

Ingest rasterized aqueduct 30 indicators to earthengine. 

Author: Rutger Hofste
Date: 20190117
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0
SCRIPT_NAME = "Y2019M01D17_RH_GA_Zonal_Stats_Weighted_Indicators_EE_V01"
OUTPUT_VERSION = 6

EE_ZONES_PATH = "projects/WRI-Aquaduct/Y2018D12D17_RH_GADM36L01_EE_V01/output_V06/gadm36l01"

EE_WEIGHTS = {}
EE_WEIGHTS["Tot"] ="projects/WRI-Aquaduct/Y2019M01D08_RH_Total_Demand_EE_V01/output_V04/global_historical_PTotWW_year_m_30s_1960_2014"
EE_WEIGHTS["Dom"] ="projects/WRI-Aquaduct/Y2019M01D08_RH_Total_Demand_EE_V01/output_V04/global_historical_PDomWW_year_m_30s_1960_2014"
EE_WEIGHTS["Ind"] ="projects/WRI-Aquaduct/Y2019M01D08_RH_Total_Demand_EE_V01/output_V04/global_historical_PIndWW_year_m_30s_1960_2014"
EE_WEIGHTS["Irr"] ="projects/WRI-Aquaduct/Y2019M01D08_RH_Total_Demand_EE_V01/output_V04/global_historical_PIrrWW_year_m_30s_1960_2014"
EE_WEIGHTS["Liv"] ="projects/WRI-Aquaduct/Y2019M01D08_RH_Total_Demand_EE_V01/output_V04/global_historical_PLivWW_year_m_30s_1960_2014"
EE_WEIGHTS["One"] ="projects/WRI-Aquaduct/Y2017M09D05_RH_Create_Area_Image_EE_V01/output_V07/global_ones_30s_V07"


EE_INDICATOR_PATH = "projects/WRI-Aquaduct/Y2019M01D10_RH_GA_Rasterize_Indicators_EE_V01/output_V01/"

GCS_BUCKET= "aqueduct30_v01"
GCS_OUTPUT_PATH = "{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("EE_ZONES_PATH: " + EE_ZONES_PATH +
      "\nEE_INDICATOR_PATH : " + EE_INDICATOR_PATH +
      "\nGCS_OUTPUT_PATH : " + GCS_OUTPUT_PATH + 
      "\nee_output_path: " + ee_output_path )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import ee


# In[4]:

ee.Initialize()


# In[5]:

zones = ee.FeatureCollection(EE_ZONES_PATH)


# In[6]:

CRS_TRANSFORM_30S_NOPOLAR = [
    0.008333333333333333,
    0,
    -180,
    0,
    -0.008333333333333333,
    89.5
]


# In[7]:

def drop_geometry(feature):
    feature_out = ee.Feature(None,{})
    feature_out = feature_out.copyProperties(source=ee.Feature(feature),
                                             properties=["gid_1","sum"])
    return feature_out
    


# In[8]:

sectors = ["One","Tot","Dom","Ind","Irr","Liv"]
#indicators = ["bws","bwd","iav","sev","gtd","drr","rfr","cfr","ucw","cep","udw","usa","rri"]
indicators = ["bws","bwd","iav","sev"]


# In[9]:

for sector in sectors:
    print(sector)
    weights = ee.Image(EE_WEIGHTS[sector])
    fc_weights_sums = weights.reduceRegions(collection=zones,
                                            reducer=ee.Reducer.sum(),
                                            crs="EPSG:4326",
                                            crsTransform=CRS_TRANSFORM_30S_NOPOLAR
                                            )
    fc_weights_sums_nogeom = fc_weights_sums.map(drop_geometry)
    output_file_path_weights = "{}/{}_weights_sum".format(GCS_OUTPUT_PATH,sector)
    task = ee.batch.Export.table.toCloudStorage(collection=fc_weights_sums_nogeom,
                                                description="{}_weights".format(sector),
                                                bucket=GCS_BUCKET,
                                                fileNamePrefix=output_file_path_weights,
                                                fileFormat="CSV")

    task.start()

    for indicator in indicators:
        print(sector,indicator)
        values_path = "{}{}".format(EE_INDICATOR_PATH,indicator)
        values = ee.Image(values_path)
        weighted_values = weights.multiply(values)

        fc_weighted_values_sums = weighted_values.reduceRegions(collection=zones,
                                                                reducer=ee.Reducer.sum(),
                                                                crs="EPSG:4326",
                                                                crsTransform=CRS_TRANSFORM_30S_NOPOLAR
                                                                )

        fc_weighted_values_sums_nogeom = fc_weighted_values_sums.map(drop_geometry)
        output_file_path_weighted_values = "{}/{}_weighted_{}_sum".format(GCS_OUTPUT_PATH,sector,indicator)
        task = ee.batch.Export.table.toCloudStorage(collection=fc_weighted_values_sums_nogeom,
                                                    description=indicator,
                                                    bucket=GCS_BUCKET,
                                                    fileNamePrefix=output_file_path_weighted_values,
                                                    fileFormat="CSV")
        task.start()


# In[10]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 0:01:58.648632
# 
# Tasks take appr. 6 min to complete

# In[ ]:



