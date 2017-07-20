FROM rutgerhofste/aqueduct30:v14

MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get install libffi-dev libssl-dev -y

ADD requirements.txt .

RUN pip3 install -r requirements.txt

RUN apt-get install python-gdal -y

RUN jupyter nbextension enable --py --sys-prefix widgetsnbextension


