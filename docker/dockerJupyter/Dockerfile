FROM rutgerhofste/conda:latest
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

ENV PATH /opt/anaconda3/bin:$PATH

# https://docs.continuum.io/anaconda/user-guide/tasks/use-jupyter-notebook-extensions
RUN conda install nb_conda -y

# Create python virtualenv in Conda
RUN conda create -n python27 python=2.7 -y
RUN conda create -n python35 python=3.5 -y
RUN conda create -n python36 python=3.6 -y
RUN conda create -n python36arc python=3.6 -y

# install packages
RUN conda install --name python36arc -c esri arcgis -y
# RUN jupyter nbextension enable arcgis --py --sys-prefix

VOLUME /volumes/data

# setup folder structure for config file and keys for SSL
RUN mkdir /root/.jupyter
RUN mkdir /.keys
RUN chmod 400 /.keys

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension

# Cheatsheet 
# Standard jupyter 
# 	/root/anaconda3/bin/jupyter
# check kernels and installs
# 	jupyter kernelspec list
