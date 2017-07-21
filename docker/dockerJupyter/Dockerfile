FROM rutgerhofste/conda:v02
MAINTAINER Rutger Hofste <rutger.hofste@wri.org>

# Create python virtualenv in Conda
RUN conda create -n python2 python=2.7 anaconda -y
RUN conda create -n python3 python=3.6 anaconda -y
RUN conda create -n python3arc python=3.6 anaconda -y

# install packages
RUN source activate python3arc
RUN conda install --name python3arc -c esri arcgis -y
RUN jupyter nbextension enable arcgis --py --sys-prefix

# Register kernels in Ipython 
RUN source activate python2 
RUN python -m ipykernel install --user --name Python2 --display-name "Python2 (conda)"
RUN source activate python3  
RUN python -m ipykernel install --user --name Python3 --display-name "Python3 (conda)"
RUN source activate python3arc
RUN python -m ipykernel install --user --name Python3arc --display-name "Python3arc (conda)"

# check kernels and installs
# jupyter kernelspec list
# pip -V
# pip3 -V

EXPOSE 8888
