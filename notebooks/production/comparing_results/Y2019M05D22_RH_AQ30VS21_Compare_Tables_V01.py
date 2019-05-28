
# coding: utf-8

# In[1]:

""" Ingest rasterized AQ30 indicators to earthengine.
-------------------------------------------------------------------------------

Compare Aqueduct 30 vs 21 and create change tables.

Creating an area image in GDAL / rasterion is cumbersome. Therefore I will 
explore options to get the task done in EarthEngine.


Author: Rutger Hofste
Date: 20190522
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0

SCRIPT_NAME = "Y2019M05D22_RH_AQ30VS21_Compare_Tables_V01"
OUTPUT_VERSION = 5

AQ21_EE_PATH = "projects/WRI-Aquaduct/Y2019M05D22_RH_AQ30VS21_Rasters_AQ21_Ingest_EE_V01/output_V01/"
AQ30_EE_PATH = "projects/WRI-Aquaduct/Y2019M05D22_RH_AQ30VS21_Rasters_AQ30_Ingest_EE_V01/output_V02/"

# Path for Overall water risk valid weight fraction.
AQ30_EE_OWR_WF_PATH = "projects/WRI-Aquaduct/Y2019M05D22_RH_AQ30VS21_Rasters_AQ30_Ingest_EE_V01/output_V02/owr_wf"

# Aqueduct 3.0 and 2.1 corresponding indicators. 
# Please note that for overall water risk in Aqueduct 3.0, a valid fraction need to be applied

INDICATORS = {"owr_score":"DEFAULT",
              "bws_score":"BWS_s",
              "iav_score":"WSV_s",
              "sev_score":"SV_s"}

# Fraction of the framework that should be valid for overall water risk.
AQ30_OWR_THRESHOLD = 0.75

CRS = "EPSG:4326"

CRS_TRANSFORM_30S = [
    0.008333333333333333,
    0,
    -180,
    0,
    -0.008333333333333333,
    90
]

GCS_BUCKET = "aqueduct30_v01"

print("AQ21_EE_PATH: " + AQ21_EE_PATH +
      "\nAQ30_EE_PATH: " +AQ30_EE_PATH )


# In[2]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[3]:

import ee
ee.Initialize()


# In[4]:

def get_aq21_image(indicator):
    path = "{}{}".format(AQ21_EE_PATH,indicator)
    return ee.Image(path)

def get_aq30_image(indicator):
    """
    Gets the Aqueduct indicator layer. If the layer is owr, it 
    applies a valid mask greate than or equal to AQ30_OWR_THRESHOLD
    
    """
    path = "{}{}".format(AQ30_EE_PATH,indicator)
    image = ee.Image(path)
    if indicator == "owr_score":
        # Mask owr by valid
        mask = ee.Image(AQ30_EE_OWR_WF_PATH)
        image = image.mask(mask.gte(AQ30_OWR_THRESHOLD))
    else:
        pass    
    return image
    

def mask_image(image,lower_bound):
    """ Create a boolean mask of an image using a [lower_bound,upper_bound). 
    The upper bound = lower_bound + 1 
    
    The mask is 1 where values in the image are lower<=x<upper and 0 elsewhere
    
    """
    upper_bound = lower_bound + 1
    lower_mask = image.gte(lower_bound)
    upper_mask = image.lt(upper_bound)
    mask = lower_mask.multiply(upper_mask)
    return mask

def sum_raster(image):
    """
    Global sum
    """
    reducer = ee.Reducer.sum()
    #geometry = ee.Geometry.Polygon(coords=[[-10.0, -10.0], [10,  -10.0], [10, 10], [-10,10]], proj= ee.Projection('EPSG:4326'),geodesic=False ) 
    geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )
    dictje =ee.Image(image).reduceRegion(reducer=reducer,
                                     geometry=geometry,
                                     #scale,
                                     crs=CRS,
                                     crsTransform=CRS_TRANSFORM_30S,
                                     #bestEffort,
                                     maxPixels=1e10,
                                     #tileScale)
                                     )
    dictje = dictje.set("aq30_indicator",aq30_indicator)
    dictje = dictje.set("aq21_indicator",aq21_indicator)
    dictje = dictje.set("aq30_lower_bound",aq30_lower_bound)
    dictje = dictje.set("aq21_lower_bound",aq21_lower_bound)
    feature = ee.Feature(None,dictje)
    fc = ee.FeatureCollection([feature])
    return fc

def export_fc(fc,description):
    task = ee.batch.Export.table.toCloudStorage(collection=fc,
                                              description=description,
                                              bucket=GCS_BUCKET,
                                              fileNamePrefix="{}/output_V{:02}/{}".format(SCRIPT_NAME,OUTPUT_VERSION,description),
                                              fileFormat="CSV"
                                              )
    task.start()
    
    


# In[5]:

aq21_lower_bounds = [0,1,2,3,4]
aq30_lower_bounds = [0,1,2,3,4]


# In[ ]:

for aq30_indicator, aq21_indicator in INDICATORS.items():
    aq21_image = get_aq21_image(aq21_indicator)
    aq30_image = get_aq30_image(aq30_indicator)
    

    for aq30_lower_bound in aq30_lower_bounds:
        for aq21_lower_bound in aq21_lower_bounds:
            print("aq30_indicator: ",aq30_indicator," aq21_indicator: ", aq21_indicator,                   "aq30_lower_bound: ",aq30_lower_bound, " aq21_lower_bound: ",aq21_lower_bound)
            description = "{}_{}_aq30lower{}_aq21lower{}_V{:02}".format(aq30_indicator,aq21_indicator,aq30_lower_bound,aq21_lower_bound,OUTPUT_VERSION)
            aq21_mask = mask_image(aq21_image,aq21_lower_bound)
            aq30_mask = mask_image(aq30_image,aq30_lower_bound)
            total_mask = aq21_mask.multiply(aq30_mask)
            
            area_km2 = ee.Image.pixelArea().divide(ee.Image(1e6))
            total_area_mask  = total_mask.multiply(area_km2)
            
            fc = sum_raster(total_area_mask)
            export_fc(fc,description)


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

