
# coding: utf-8

# In[ ]:

""" Store merged and simplified pandas dataframes in postGIS database. 
-------------------------------------------------------------------------------


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
OVERWRITE = 1
SCRIPT_NAME = "Y2018M05D23_RH_Simplify_DataFrames_Pandas_30sPfaf06_V02"
OUTPUT_VERSION = 4

DATABASE_ENDPOINT = "aqueduct30v05.cgpnumwmfcqc.eu-central-1.rds.amazonaws.com"
DATABASE_NAME = "database01"

# Area 
OUTPUT_TABLE_NAME = "area_m2_30spfaf06"

