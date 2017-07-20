FROM rutgerhofste/aqueduct30:v12

MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get install libffi-dev libssl-dev python-dev python-pip -y

ADD requirements.txt .

RUN pip install -r requirements.txt

RUN apt-get install python-gdal -y

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension

RUN apt-get install software-properties-common

RUN add-apt-repository ppa:certbot/certbot -y

RUN apt-get update

RUN apt-get install certbot -y



