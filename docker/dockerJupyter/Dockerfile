FROM rutgerhofste/conda:v03
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

ENV PATH /root/anaconda3/bin:$PATH

# Create python virtualenv in Conda
# RUN conda create -n python2 python=2.7 anaconda -y
# RUN conda create -n python3 python=3.6 anaconda -y
# RUN conda create -n python3arc python=3.6 anaconda -y

# install packages
# RUN conda install --name python3arc -c esri arcgis -y
# RUN jupyter nbextension enable arcgis --py --sys-prefix

RUN conda install --name python2 jupyter -y 
# RUN conda install --name python3 jupyter -y
# RUN conda install --name python3arc jupyter -y 

# Register kernels in Ipython (not necessary if using extension)
# RUN /root/anaconda3/envs/python2/bin/python -m ipykernel install --user --name Python2 --display-name "Python2 (conda)" 
# RUN /root/anaconda3/envs/python3/bin/python -m ipykernel install --user --name Python3 --display-name "Python3 (conda)"
# RUN /root/anaconda3/envs/python3arc/bin/python -m ipykernel install --user --name Python3arc --display-name "Python3arc (conda)"

# https://docs.continuum.io/anaconda/user-guide/tasks/use-jupyter-notebook-extensions
RUN conda install nb_conda -y


# check kernels and installs
# jupyter kernelspec list
# pip -V
# pip3 -V

EXPOSE 8888
