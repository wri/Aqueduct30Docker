FROM rutgerhofste/jupyter:v01
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

ENV PATH /opt/anaconda3/bin:$PATH

RUN conda install -c conda-forge jupyterhub -y
RUN conda install notebook -y

# jupyterhub --generate-config
# On Ubuntu .... sudu docker cp awesome_hugle:/jupyterhub_config.py /jupyterhub_config.py

RUN python3 -m pip install oauthenticator

# set system variables (see .env folder on local machine)
# export GITHUB_CLIENT_ID=
# export GITHUB_CLIENT_SECRET=
# export OAUTH_CALLBACK_URL= https://PublicIP:8000/hub/oauth_callback

