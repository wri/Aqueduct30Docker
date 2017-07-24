
# coding: utf-8

# # Welcome
# 
# **If you see this notebook in anything different than a jupyter notebook environment, you first need to follow instructions in the readme.md document on GitHub and set up your jupyter environment.**  
# 
# This notebook will check if your environment is all set up and will deal with the correct authorization. Aqueduct 3.0 requires quite a number of different packages and for some you need to create an account. 
# 
# * Amazon Web Services (AWS)
# * Google Earth Engine
# * (optional) GitHub if you want to contribute to Aqueduct 3.0 development
# 
# 
# Before you start, make sure that in addition to this notebook, you also have a terminal open running bash in your docker container in case you missed a step in the readme.md document. The readme document also explains how to setup this additional bash terminal. 
# 
# If you followed the readme.md file correctly, you already executed the following commands in you bash terminal
# 
# * `earthengine authenticate` (use your earthengine enabled Google Account)  
# * `aws configure` (Use the credentials that grant you access to WR's S3)
# 

# ## AWS
# 
# If you run this notebook in a Docker Container on an Amazon EC2 instance, you probably already have a AWS account. However, in order to access data on WRI's Amazon S3 bucket, you need additional credentials to access to **WRI's Bucker"**. Please contact rutger.hofste@wri.org to ask for permission to access WRI data. 
# 

# The following command will try to copy a tiny file to your current directory and clean up afterwards. Note that you can execute bash script in Jupyter with an exclamation mark. 
# 
# You can run cells by pressing shift+enter or the button in the menu

# In[1]:

get_ipython().system('aws s3 cp s3://wri-projects/Aqueduct30/test/testFile.txt /mnt/notebooks/Aqueduct30Docker/notebooks/')
get_ipython().system('if [ -f testFile.txt ] ; then echo "It worked" ; else echo "it did not work, try running \'aws configure\' in your terminal and setup credentials" ; fi')
get_ipython().system('rm testFile.txt')


# # Earth Engine
# 
# before you can use the earthengine scripts, you need to sign up for an account. This is explained in the readme.md file in the github repo. The following script only checks if your setup is working. These cells are based on the official Earth Engine notebooks developed by Tyler Erickson
# 
# 
# 

# In[2]:

# Code to check the IPython Widgets library.
try:
  import ipywidgets
  print('The IPython Widgets library (version {0}) is available on this server.'.format(
      ipywidgets.__version__
    ))
except ImportError:
  print('The IPython Widgets library is not available on this server.\n'
        'Please see https://github.com/jupyter-widgets/ipywidgets '
        'for information on installing the library.')
  raise


# In[3]:

# Code to check the Earth Engine API library.
try:
  import ee
  print('The Earth Engine Python API (version {0}) is available on this server.'.format(
      ee.__version__
    ))
except ImportError:
  print('The Earth Engine Python API library is not available on this server.\n'
        'Please see https://developers.google.com/earth-engine/python_install '
        'for information on installing the library.')
  raise


# In[4]:

# Code to check if authorized to access Earth Engine.
import cStringIO
import os
import urllib

def isAuthorized():
  try:
    ee.Initialize()
    return True
  except:
    return False

form_item_layout = ipywidgets.Layout(width="100%", align_items='center')
  
if isAuthorized():
  
  def revoke_credentials(sender):
    credentials = ee.oauth.get_credentials_path()
    if os.path.exists(credentials):
      os.remove(credentials)
    print('Credentials have been revoked.')
  
  # Define widgets that may be displayed.
  auth_status_button = ipywidgets.Button(
    layout=form_item_layout,
    disabled = True,
    description = 'The server is authorized to access Earth Engine',
    button_style = 'success',
    icon = 'check'
  )
  
  instructions = ipywidgets.Button(
    layout=form_item_layout,
    description = 'Click here to revoke authorization',
    button_style = 'danger',
    disabled = False,
  )
  instructions.on_click(revoke_credentials)

else:
  
  def save_credentials(sender):
    try:
      token = ee.oauth.request_token(get_auth_textbox.value.strip())
    except Exception as e:
      print(e)
      return
    ee.oauth.write_token(token)
    get_auth_textbox.value = ''  # Clear the textbox.
    print('Successfully saved authorization token.')

  # Define widgets that may be displayed.
  get_auth_textbox = ipywidgets.Text(
    placeholder='Paste authorization code here',
    description='Authentication Code:'
  )
  get_auth_textbox.on_submit(save_credentials)

  auth_status_button = ipywidgets.Button(
    layout=form_item_layout,
    button_style = 'danger',
    description = 'The server is not authorized to access Earth Engine',
    disabled = True
  )
  
  instructions = ipywidgets.VBox(
    [
      ipywidgets.HTML(
        'Click on the link below to start the authentication and authorization process. '
        'Once you have received an authorization code, paste it in the box below and press return.'
      ),
      ipywidgets.HTML(
        '<a href="{url}" target="auth">Open Authentication Tab</a><br/>'.format(
          url=ee.oauth.get_authorization_url()
        )
      ),
      get_auth_textbox
    ],
    layout=form_item_layout
  )

# Display the form.
form = ipywidgets.VBox([
  auth_status_button,
  instructions
])
form


# Once the server is authorized, you can retrieve data from Earth Engine and use it in the notebook.

# In[5]:

# Code to display an Earth Engine generated image.
from IPython.display import Image

url = ee.Image("CGIAR/SRTM90_V4").getThumbUrl({'min':0, 'max':3000})
Image(url=url)


# ## All set? 
# 
# please let me know if you are having difficulties with this setup. rutger.hofste@wri.org
# 
# 

# In[ ]:



