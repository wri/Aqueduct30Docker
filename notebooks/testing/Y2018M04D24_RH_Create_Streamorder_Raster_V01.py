
# coding: utf-8

# In[ ]:




# In[ ]:




# In[26]:

import sys
sys.path.append("/opt/pcraster-4.1.0_x86-64/python")
import pcraster


# In[6]:

input_file_path = "/volumes/data/Y2018M04D18_RH_Convert_Aux_Rasters_Geotiff_V01/input_V01/global_lddsound_numpad_05min.map"


# In[ ]:




# In[8]:

Ldd = pcraster.readmap(input_file_path)


# In[10]:

Result = pcraster.streamorder(Ldd)


# In[24]:

array = pcraster.pcr2numpy(Result,-9999)


# In[25]:




# In[ ]:



