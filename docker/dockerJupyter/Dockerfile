FROM rutgerhofste/conda:v03
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

ENV PATH /root/anaconda3/bin:$PATH

# Create python virtualenv in Conda
RUN conda create -n python2 python=2.7 anaconda -y
RUN conda create -n python3 python=3.6 anaconda -y
RUN conda create -n python3arc python=3.6 anaconda -y

# https://docs.continuum.io/anaconda/user-guide/tasks/use-jupyter-notebook-extensions
RUN conda install nb_conda -y

# check kernels and installs
# jupyter kernelspec list
# pip -V
# pip3 -V

EXPOSE 8888
