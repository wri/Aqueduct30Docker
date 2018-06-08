
# coding: utf-8

# In[1]:

""" QA for water stress in several basin
-------------------------------------------------------------------------------

This step was done manually using arcMap since the .gdb is not ogc compliant.

Input data: wri-projects/Aqueduct2x/Aqueduct21Data/demand
Algorithm used: ArcMap batch copy raster

Uploaded to:

wri-projects/Aqueduct30/qaData/Y2018M06D08_RH_QA_Aqueduct21_Demand_To_Geotiff_V01/
output_V01

Author: Rutger Hofste
Date: 20180604
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 1
OVERWRITE_OUTPUT = 1
SCRIPT_NAME = 'Y2018M06D08_RH_QA_Aqueduct21_Demand_To_Geotiff_V01'
OUTPUT_VERSION = 4


# In[ ]:



