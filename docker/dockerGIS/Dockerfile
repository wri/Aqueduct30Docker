FROM rutgerhofste/jupyter:latest
MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get install -y dialog apt-utils libffi-dev libssl-dev 

# Root environment 

RUN conda install -c conda-forge awscli

# Create python virtualenv in Conda
RUN conda create -n python27 python=2.7 jupyter -y
RUN conda create -n python35 python=3.5 jupyter -y
RUN conda create -n python36 python=3.6 jupyter -y
RUN conda create -n python36arc python=3.6 jupyter -y

# Installing KernelSpecs
RUN /opt/anaconda3/envs/python27/bin/python -m ipykernel install --name python27 --display-name "Python 27"
RUN /opt/anaconda3/envs/python35/bin/python -m ipykernel install --name python35 --display-name "Python 35"
RUN /opt/anaconda3/envs/python36/bin/python -m ipykernel install --name python36 --display-name "Python 36"
RUN /opt/anaconda3/envs/python36arc/bin/python -m ipykernel install --name python36arc --display-name "Python 36 Arc"

# install regular packages (main conda channel)
ADD requirementsPython27.txt .
ADD requirementsPython35.txt .
ADD requirementsPython36.txt .
ADD requirementsPython36arc.txt .

RUN conda install --name python27 --file requirementsPython27.txt
RUN conda install --name python35 --file requirementsPython35.txt
RUN conda install --name python36 --file requirementsPython36.txt
RUN conda install --name python36arc --file requirementsPython36arc.txt


# use source so the cli will be available when you activate the python27 environment
RUN [ "/bin/bash", "-c", "source activate python27 && pip install google-api-python-client" ] 
RUN [ "/bin/bash", "-c", "source activate python27 && pip install earthengine-api" ]

# make command line tool accessible to root python (note that the path for cli will be part of root env)
RUN pip install google-api-python-client
RUN pip install earthengine-api


# install packages from conda forge channel 
RUN conda install --name python35 -c conda-forge geopandas -y
RUN conda install --name python36 -c conda-forge geopandas -y

RUN conda install --name python36arc -c esri arcgis -y
RUN conda install -c esri arcgis -y
RUN jupyter nbextension enable arcgis --py --sys-prefix

# Reinstall core jupyter and conda extension. better be safe than sorry 
RUN conda install jupyter -y
RUN conda install nb_conda -y
RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension

# Docker uses POSIX which causes conflicts with TMUX, install en_US locale UTF-8
RUN apt-get install --reinstall -y locales
RUN sed -i 's/# en_US.UTF-8 en_US.UTF-8/en_US.UTF-8-8 UTF-8/' /etc/locale.gen 
RUN locale-gen en_US.UTF-8 
RUN apt-get install tmux -y

# Finally because of the backward compatibility issues
RUN apt-get install python-gdal -y

RUN conda install --name python27 gdal -y
RUN conda install --name python35 gdal -y

RUN conda clean -yt
