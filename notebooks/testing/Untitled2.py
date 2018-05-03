
# coding: utf-8

# In[1]:

import ee


# In[2]:

ee.Initialize()


# In[8]:

def add_image_property_prefix(image,prefix):
    """ Add prefix to properties
    
    Args:
        image (ee.Image) : image with properties.
        prefix (string) : prefix string.
    
    Returns:
        dummy_image (ee.Image) : Null image with renamed properties
    
    """
    image = ee.Image(image)
    original_keys = image.propertyNames()
    values = original_keys.map(lambda x: ee.String(image.get(x)))
    prefixed_keys = original_keys.map(lambda x: ee.String(prefix).cat(x))
    prefixed_properties = ee.Dictionary.fromLists(prefixed_keys,values)
    dummy_image = ee.Image(None).setMulti(prefixed_properties)
    return dummy_image


# In[9]:

def zonal_stats_image_propertes(i_zones,i_values,i_result,extra_properties={},zones_prefix="zones_",values_prefix="values_"):
    """ Copy the properties of the zonal image and value image to the result image
    -------------------------------------------------------------------------------
    
    Args:
        i_zones (ee.Image) : Zones image.
        i_values (ee.Image) : Values image.
        i_result (ee.Image) : Result image from raster zonal stats.
        extra_properties (dict) : Dictionary with extra properties.
        zones_prefix (string) : Prefix for zones image. Defaults to 'zones_'
        values_prefix (string) : Prefix for values image. Defaults to 'values_'
        
    
    TODO:
        Add option to include weigth image.
 
    """
    
    i_dummy_zones_prefixed = add_image_property_prefix(i_zones,zones_prefix)
    i_dummy_values_prefixed = add_image_property_prefix(i_values,values_prefix)   
    i_result = i_result.copyProperties(i_dummy_zones_prefixed).copyProperties(i_dummy_values_prefixed)
    return ee.Image(i_result)


# In[10]:

i_values = ee.Image("projects/WRI-Aquaduct/Y2017M09D05_RH_Create_Area_Image_EE_V01/output_V07/global_area_m2_5min_V07")


# In[11]:

i_zones = ee.Image("projects/WRI-Aquaduct/Y2018M04D20_RH_Ingest_HydroBasins_GCS_EE_V01/output_V02/hybas_lev06_v1c_merged_fiona_30s_V04")


# In[14]:

i_result = ee.Image(1)


# In[15]:

i_result = zonal_stats_image_propertes(i_zones,i_values,i_result)


# In[16]:

i_result.getInfo()


# In[ ]:



