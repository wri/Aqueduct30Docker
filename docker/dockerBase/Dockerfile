FROM rutgerhofste/dockerjupyter:v05
MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

ADD requirements.txt .
RUN pip3 install -r requirements.txt

VOLUME /volumes/data

# Password protect jupyter
RUN mkdir /root/.jupyter

RUN git clone https://github.com/rutgerhofste/Aqueduct30Docker.git

# config with hashed password certificate and key enabled 
# RUN cp /Aqueduct30Docker/jupyter_notebook_config.py /root/.jupyter/.

