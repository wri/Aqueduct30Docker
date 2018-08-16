
# coding: utf-8

# In[1]:

""" Server side ftw


"""

SCRIPT_NAME = "Y2018M08D14_RH_Postprocess_BRP_V01"
OUTPUT_VERSION = 2

EE_INPUT_PATH = "users/rutgerhofste/Y2018M08D13_RH_Ingest_SHP_Earthengine_V01/output_V03/"
EE_OUTPUT_PATH = "users/rutgerhofste/"

EXTRA_PROPERTIES = {"script_used":SCRIPT_NAME,
                    "download_date":"2018-08-13",
                    "download_url":"http://www.nationaalgeoregister.nl/geonetwork/srv/dut/catalog.search#/metadata/%7B25943e6e-bb27-4b7a-b240-150ffeaa582e%7D?tab=relations"}


# In[2]:

import ee
ee.Initialize()


# In[3]:

def fix_data_type(feature):
    # fixes required due to shapefile limitations
    
    feature_out = ee.Feature(feature)
    feature_out = feature_out.set("CAT_GEWASCATEGORIE",feature.get("CAT_GEWASC"))
    feature_out = feature_out.set("GWS_GEWAS",feature.get("GWS_GEWAS"))
    feature_out = feature_out.set("GWS_GEWASCODE",ee.Number.parse(ee.String(feature.get("GWS_GEWASC"))))
    feature_out = feature_out.set("GEOMETRIE_Length",feature.get("GEOMETRIE_"))
    feature_out = feature_out.set("GEOMETRIE_Area",feature.get("GEOMETRI_1"))
    
    # additional properties
    feature_out = feature_out.set("year",year)
    
    # filter 
    feature_out2 = feature_out.select(["CAT_GEWASCATEGORIE","GWS_GEWAS","GWS_GEWASCODE","GEOMETRIE_Length","GEOMETRIE_Area","year"])
    
    return ee.Feature(feature_out2)


# In[4]:

filenames = {}
fc_merge = ee.FeatureCollection(ee.Feature(None,{}))
for year in range(2009,2019):
    if year != 2018:
        filename = "BRP_Gewaspercelen_{:04.0f}".format(year)
    elif year == 2018:
        filename = "BRP_Gewaspercelen_{:04.0f}_concept".format(year)
    fc = ee.FeatureCollection(EE_INPUT_PATH+filename)
    fc_out = ee.FeatureCollection(fc.map(fix_data_type))
    fc_merge = fc_merge.merge(fc_out)


# In[5]:

fc_merge = ee.FeatureCollection(fc_merge.setMulti(EXTRA_PROPERTIES))


# In[6]:

fc_merge.size().getInfo()


# In[7]:

task = ee.batch.Export.table.toAsset(fc_merge,
                                     description="Y2018M08D14_RH_Postprocess_BRP_V01",
                                     assetId = "{}Basisregistratie_Gewaspercelen_V05".format(EE_OUTPUT_PATH))


# In[8]:

task.start()


# In[ ]:



