FROM rutgerhofste/condajupyter:v16ubuntu
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

ENV PATH /root/anaconda3/bin:$PATH

EXPOSE 8000

RUN conda install -c conda-forge jupyterhub -y
RUN conda install notebook -y

# jupyterhub --generate-config
# On Ubuntu .... sudu docker cp awesome_hugle:/jupyterhub_config.py /jupyterhub_config.py

RUN python3 -m pip install oauthenticator

