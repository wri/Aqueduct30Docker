FROM ubuntu:16.10
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

RUN apt-get update
RUN apt-get install -y git
RUN apt-get install wget
RUn apt-get install bzip2

RUN wget https://repo.continuum.io/archive/Anaconda3-4.4.0-Linux-x86_64.sh 
RUN bash Anaconda3-4.4.0-Linux-x86_64.sh -b -p /opt/anaconda3

ENV PATH /opt/anaconda3/bin:$PATH

EXPOSE 8000 
EXPOSE 8888

