FROM eboraas/jupyter
MAINTAINER Rutger Hofste <rutgerhofste@gmail.com>

RUN apt-get update
RUN apt-get install -y git

RUN git clone https://github.com/rutgerhofste/Aqueduct30Docker.git

RUN pip install --upgrade pip

ADD requirements.txt .
RUN pip install -r requirements.txt

VOLUME /volumes/data

# Password protect jupyter
RUN mkdir /root/.jupyter

RUN cp /mnt/notebooks/Aqueduct30Docker/jupyter_notebook_config.py /root/.jupyter/.

