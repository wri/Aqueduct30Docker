FROM rutgerhofste/aqueduct30:v15

MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get install libffi-dev libssl-dev -y

ADD requirements.txt .

RUN pip install -r requirements.txt
RUN pip3 install -r requirements.txt

RUN apt-get install python-gdal -y




