FROM rutgerhofste/jupyter:v02
MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get install dialog apt-utils -y

RUN apt-get install libffi-dev libssl-dev -y
RUN apt-get install screen

RUN conda install -c conda-forge awscli

ADD requirementsPython2.txt .
ADD requirementsPython3.txt .
ADD requirementsPython3arc.txt .

RUN conda install --name python2 --file requirementsPython2.txt
RUN conda install --name python3 --file requirementsPython3.txt
RUN conda install --name python3arc --file requirementsPython3arc.txt

# Non standard Conda Packages (not part of Conda Repo)
RUN conda install --name python2 -c conda-forge google-api-python-client
RUN conda install --name python2 -c tylerickson earthengine-api

RUN conda install --name python3arc -c esri arcgis -y
# RUN jupyter nbextension enable arcgis --py --sys-prefix

RUN conda install jupyter -y
RUN conda install nb_conda -y

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension

# Docker uses POSIX which causes conflicts with TMUX, install en_US locale UTF-8
RUN apt-get install --reinstall -y locales
RUN sed -i 's/# en_US.UTF-8 en_US.UTF-8/en_US.UTF-8-8 UTF-8/' /etc/locale.gen 
RUN locale-gen en_US.UTF-8 

RUN apt-get install tmux
