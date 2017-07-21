FROM rutgerhofste/dockerjupyter:v07
MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

ADD requirements.txt .
RUN pip3 install -r requirements.txt
RUN pip install -r requirements.txt

VOLUME /volumes/data

# setup folder structure for config file and keys for SSL
RUN mkdir /root/.jupyter
RUN mkdir /.keys
RUN chmod 400 /.keys

RUN git clone https://github.com/rutgerhofste/Aqueduct30Docker.git