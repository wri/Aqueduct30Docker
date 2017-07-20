FROM ubuntu:16.10
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

RUN apt-get update
RUN apt-get install -y git


# Python 3.5.2+
RUN apt-get install python3=3.5.2+ -y

# 8.1.2
RUN apt-get install -y python3-pip=8.1.2

RUN pip3 install --upgrade pip

# 4.3.0 (Jupyter core)
RUN pip3 install jupyter

# 5.0.0 (jupyter notebook --version)
RUN pip3 install --upgrade notebook


# Python 2.7.12
# RUN apt-get install python=2.7.12 -y

# pip 8.1.1
# RUN apt-get install python-pip -y

# RUN pip install --upgrade pip

# RUN apt-get install python-dev

# RUN apt-get install jupyter

EXPOSE 8888

# jupyter notebook --no-browser --ip=0.0.0.0 --notebook-dir=/notebooks --allow-root