FROM ubuntu:16.10
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

RUN apt-get update
RUN apt-get install -y git


# Python 3.5.2+
RUN apt-get install python3 -y

# 8.1.2
RUN apt-get install -y python3-pip

RUN pip3 install --upgrade pip


# 4.3.0 (Jupyter core)
RUN pip3 install jupyter

# 5.0.0 (jupyter notebook --version)
# IPython 6.0 does not support Python 2.x 
RUN pip3 install --upgrade notebook

EXPOSE 8888
