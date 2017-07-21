FROM rutgerhofste/jupyter:v03
MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get install libffi-dev libssl-dev -y

ADD requirementsPython2.txt .
ADD requirementsPython3.txt .
ADD requirementsPython3arc.txt .

RUN conda install --name python2 -r requirementsPython2.txt
RUN conda install --name python3 -r requirementsPython3.txt
RUN conda install --name python3arc -r requirementsPython3arc.txt

# RUN apt-get install python-gdal -y

VOLUME /volumes/data

# setup folder structure for config file and keys for SSL
RUN mkdir /root/.jupyter
RUN mkdir /.keys
RUN chmod 400 /.keys

RUN git clone https://github.com/rutgerhofste/Aqueduct30Docker.git

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension