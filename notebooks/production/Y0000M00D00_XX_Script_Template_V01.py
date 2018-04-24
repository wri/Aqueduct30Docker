
# coding: utf-8

# In[6]:

""" Short description of the notebook. 
-------------------------------------------------------------------------------
Notebooks in the Aqueduct Project are used in production. This means that the   
notebooks should execute completely by pressing restart and run in the menu.
Commands should not exceed the 80 character limit which is the length of this  |

Code follows the Google for Python Styleguide. Exception are the scripts that 
use earth engine since this is camelCase instead of underscore.


Author: Rutger Hofste
Date: 20180327
Kernel: python35
Docker: rutgerhofste/gisdocker:ubuntu16.04

Args:

    SCRIPT_NAME (string) : Script name.
    OUTPUT_VERSION (integer) : Output version.


Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y0000M00D00_XX_Script_Template_V01"
OUTPUT_VERSION = 1

# Output Parameters



# In[7]:

import time, datetime, sys, logging
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


# In[14]:

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler("./logs/{}V{:02.0f}.log".format(SCRIPT_NAME,OUTPUT_VERSION))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# In[15]:

logger.debug("test")


# In[ ]:

# Imports

# ETL

# Functions

# Script


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# In[ ]:

Previous runs:  
0:24:15.930678    

