
# coding: utf-8

# In[ ]:

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

    SCRIPT_NAME (string) : Script name


Returns:


"""

# Input Parameters

SCRIPT_NAME = "Y0000M00D00_XX_Script_Template_V01"

# Output Parameters



# In[ ]:

import time, datetime, sys
dateString = time.strftime("Y%YM%mD%d")
timeString = time.strftime("UTC %H:%M")
start = datetime.datetime.now()
print(dateString,timeString)
sys.version


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

