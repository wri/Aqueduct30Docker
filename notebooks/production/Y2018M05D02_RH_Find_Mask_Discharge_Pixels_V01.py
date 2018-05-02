
# coding: utf-8

# In[4]:

""" Find the pixels to mask within each basin to use for river discharge. 
-------------------------------------------------------------------------------

The riverdischarge data from PCRGLOBWB is cummulative runoff. The available
discharge is the maximum discharge with a few exceptions. These exceptions
occur as a result of a mismatch of the PCRGLOBWB local drainage direction and
the hydrobasin level 6 subbasins. 

- A basin has a few cells that belong to another stream. 
    Often close to the most downstream pixel.
- A basin has a few cells that are an endorheic upstream basin.

There are several subbasin types:

1) Large stream in only one cell. perpendicular contributing areas. 
    e.g.: 172265.
2) Large stream in a few cells, perpendicular contributing areas. 
    e.g.: 172263.
3) Tiny basin smaller than one 5min cell.
    e.g.: 172261.
4) Large basin with main stream.
    e.g.: 172250.
5) Large basin with main stream and other stream in most downstream cell. 
    e.g.: 172306.
6) Small basin with a confluence within basin. Stream_order increases in most 
    downstream cell but is part of basin. 
    e.g.: 172521.
7) Basin with an endorheac basin in one of its upstream cells.
    e.g.: 172144.
8) Large basin with main stream and other stream with large flow but lower 
    stream order.
    e.g.: To be found. 
    
The cell of highest streamorder will be masked for 5) and 7)
No mask will be applied to the other categories. 

Author: Rutger Hofste
Date: 20180502
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : Output version.


Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y2018M05D02_RH_Find_Mask_Discharge_Pixels_V01"
STREAM_ORDER_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D25_RH_Ingest_Pcraster_GCS_EE_V01/output_V01/global_streamorder_dimensionless_05min_V02"
HYBAS_LEV06_ASSET_ID = "projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V01/hybas_lev06_v1c_merged_fiona_30s_V04"
GLOBAL_AREA_M2_30S_ASSET_ID = "projects/WRI-Aquaduct/PCRGlobWB20_Aux_V02/global_area_m2_30s_V05"

OUTPUT_VERSION = 1

ee_output_path = "projects/WRI-Aquaduct/{}/output_V{:02.0f}".format(SCRIPT_NAME,OUTPUT_VERSION)

print("Output ee: " +  ee_output_path)



# In[2]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[ ]:

# ETL

