FROM eboraas/jupyter
MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get update
RUN apt-get install -y git

RUN git clone https://github.com/rutgerhofste/Aqueduct30Docker.git

RUN pip install boto3

RUN pip install awscli

RUN pip install pandas

VOLUME /volumes/data

# Password protect jupyter
RUN mkdir /root/.jupyter

RUN cp /mnt/notebooks/Aqueduct30Docker/jupyter_notebook_config.py /root/.jupyter/.


