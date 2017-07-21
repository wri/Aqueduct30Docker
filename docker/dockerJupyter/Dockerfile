FROM ubuntu:16.10
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

RUN apt-get update
RUN apt-get install -y git

# Python 2 and Jupyter (legacy that can run python2)
RUN apt-get install python python-pip -y
RUN pip install --upgrade pip
RUN pip install IPython==5.0
RUN pip install jupyter

# Python 3.5.2+
RUN apt-get install python3 -y

# 8.1.2
RUN apt-get install -y python3-pip
RUN pip3 install --upgrade pip
# Fix Pip paths for Python 2
RUN python -m pip install -U --force-reinstall pip


# Add Kernel
RUN python3 -m pip install ipykernel


# check kernels and installs
# jupyter kernelspec list
# pip -V
# pip3 -V

EXPOSE 8888
