
# coding: utf-8

# In[8]:

from gbdxtools.catalog import Catalog
from gbdxtools.images.catalog_image import CatalogImage
import rasterio

imageId = "10400E0001DB6A00"
crop = "POLYGON ((36.9392906658426 33.9164103054402,36.943294798074 33.9164103054402,36.943294798074 33.9124061732088,36.9392906658426 33.9124061732088,36.9392906658426 33.9164103054402))"

print(Catalog().get_strip_metadata(imageId))


# In[9]:

c = CatalogImage(imageId, product="ortho")
aoi = c.aoi(wkt=crop)
image = aoi.geotiff(path="/volumes/data/output.tif")
aoi.plot()


# In[ ]:



