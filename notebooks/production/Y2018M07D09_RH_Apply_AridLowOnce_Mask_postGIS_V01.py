
# coding: utf-8

# In[1]:

""" Apply the mask for arid and lowwater use subbasins based on ols_ols10 (once).
-------------------------------------------------------------------------------

Join the results of the arid and lowwater use mask based on annual values (ols)
(ols_ols10_**) and the master table. 


Author: Rutger Hofste
Date: 20180628
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:
    TESTING (Boolean) : Toggle testing case.
    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : output version.
    DATABASE_ENDPOINT (string) : RDS or postGreSQL endpoint.
    DATABASE_NAME (string) : Database name.
    TABLE_NAME_AREA_30SPFAF06 (string) : Table name used for areas. Must exist
        on same database as used in rest of script.
    S3_INPUT_PATH_RIVERDISCHARGE (string) : AWS S3 input path for 
        riverdischarge.    
    S3_INPUT_PATH_DEMAND (string) : AWS S3 input path for 
        demand.     

"""

TESTING = 0
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M07D09_RH_Apply_AridLowOnce_Mask_postGIS_V01'
OUTPUT_VERSION = 1

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

INPUT_TABLE_NAME_LEFT

INPUT_TABLE_NAME_RIGHT = 'y2018m06d04_rh_water_stress_postgis_30spfaf06_v02_v05'
OUTPUT_TABLE_NAME = SCRIPT_NAME.lower() + "_v{:02.0f}".format(OUTPUT_VERSION)


# In[ ]:



