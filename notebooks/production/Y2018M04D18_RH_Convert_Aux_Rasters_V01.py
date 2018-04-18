
# coding: utf-8

# In[ ]:

""" Convert auxiliary files like DEM, LDD etc. to geotiff. Store on GCS.
-------------------------------------------------------------------------------

In addition to the time-series and non-time-series data from PCRGLOBWB 
auxilirary data files were shared that contain crucial information such as 
digital elevation model (DEM), local drainage direction (ldd) etc.

The files are renamed, converted to geotiffs and uploaded to GCS.

Args:
    SCRIPT_NAME (string) : Script Name.
    

