FROM rutgerhofste/jupyter:v02
MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get install dialog apt-utils -y

RUN apt-get install libffi-dev libssl-dev -y
RUN apt-get install screen

RUN conda install -c conda-forge awscli

# Create python virtualenv in Conda
RUN conda create -n python27 python=2.7 -y
RUN conda create -n python35 python=3.5 -y
RUN conda create -n python36 python=3.6 -y
RUN conda create -n python36arc python=3.6 -y

# install regular packages
ADD requirementsPython27.txt .
ADD requirementsPython35.txt .
ADD requirementsPython36.txt .
ADD requirementsPython36arc.txt .

RUN conda install --name python27 --file requirementsPython27.txt
RUN conda install --name python35 --file requirementsPython35.txt
RUN conda install --name python36 --file requirementsPython36.txt
RUN conda install --name python36arc --file requirementsPython3arc.txt

# Conda Forge Channel
RUN conda install --name python27 -c conda-forge google-api-python-client
RUN conda install --name python27 -c tylerickson earthengine-api

RUN conda install --name python35 -c conda-forge geopandas -y
RUN conda install --name python36 -c conda-forge geopandas -y


RUN conda install --name python36arc -c esri arcgis -y
# RUN jupyter nbextension enable arcgis --py --sys-prefix

RUN conda install jupyter -y
RUN conda install nb_conda -y

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension

# Docker uses POSIX which causes conflicts with TMUX, install en_US locale UTF-8
RUN apt-get install --reinstall -y locales
RUN sed -i 's/# en_US.UTF-8 en_US.UTF-8/en_US.UTF-8-8 UTF-8/' /etc/locale.gen 
RUN locale-gen en_US.UTF-8 

RUN apt-get install tmux -y
