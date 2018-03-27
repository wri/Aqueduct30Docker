
# coding: utf-8

# In[ ]:

""" Short description of the notebook. 

Notebooks in the Aqueduct Project are used in production. This means that the   
notebooks should execute completely by pressing restart and run in the menu.
Commands should not exceed the 80 character limit which is the length of this  |

Author: Rutger Hofste
Date: 20180327

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


# In[1]:

# ETL


# In[ ]:

# Imports


# In[2]:

# Functions


# In[3]:

# Script


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)

