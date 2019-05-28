
# coding: utf-8

# In[1]:

""" Generate and export difference maps of various indicators.
-------------------------------------------------------------------------------

Exports at 5 arc minutes for print. The original data is available at 30 arc
seconds


Author: Rutger Hofste
Date: 20190528
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

"""

TESTING = 0

SCRIPT_NAME = "Y2019M05D28_RH_AQ30VS21_Export_Dif_Geotiff_EE_V01"
OUTPUT_VERSION = 1

AQ21_EE_PATH = "projects/WRI-Aquaduct/Y2019M05D22_RH_AQ30VS21_Rasters_AQ21_Ingest_EE_V01/output_V01/"
AQ30_EE_PATH = "projects/WRI-Aquaduct/Y2019M05D22_RH_AQ30VS21_Rasters_AQ30_Ingest_EE_V01/output_V02/"

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

CRS_TRANSFORM_5MIN = [
    0.08333333333333333,
    0,
    -180,
    0,
    -0.08333333333333333,
    90
]

GCS_BUCKET = "aqueduct30_v01"

print("AQ21_EE_PATH: " + AQ21_EE_PATH +
      "\nAQ30_EE_PATH: " +AQ30_EE_PATH +
      "\nGCS_BUCKET: " + GCS_BUCKET)


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





# In[5]:

for aq30_indicator, aq21_indicator in INDICATORS.items():
    print(aq30_indicator,aq21_indicator)
    aq21_image = get_aq21_image(aq21_indicator)
    aq30_image = get_aq30_image(aq30_indicator)
    
    i_aq30_minus_aq21 = aq30_image.subtract(aq21_image)
    
    description = "{}{}".format(aq30_indicator,aq21_indicator)
    fileNamePrefix = "{}/output_V{:02f}/{}_minus_{}".format(SCRIPT_NAME,OUTPUT_VERSION,aq30_indicator,aq21_image )
    
    #geometry = ee.Geometry.Polygon(coords=[[-10.0, -10.0], [10,  -10.0], [10, 10], [-10,10]], proj= ee.Projection('EPSG:4326'),geodesic=False ) 
    geometry = ee.Geometry.Polygon(coords=[[-180.0, -90.0], [180,  -90.0], [180, 90], [-180,90]], proj= ee.Projection('EPSG:4326'),geodesic=False )
    
    task = ee.batch.Export.image.toCloudStorage(image=i_aq30_minus_aq21,
                                                description=description,
                                                bucket=GCS_BUCKET,
                                                fileNamePrefix=fileNamePrefix ,
                                                #dimensions,
                                                #region=geometry,
                                                #scale,
                                                crs=CRS,
                                                crsTransform=CRS_TRANSFORM_5MIN,
                                                maxPixels=1e10,
                                                #shardSize,
                                                #fileDimensions,
                                                #skipEmptyTiles,
                                                fileFormat="GeoTIFF",
                                                #formatOptions
                                                )
    task.start()


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# previous run:
