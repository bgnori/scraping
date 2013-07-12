#!/bin/sh

DISTRIBTE_VER=0.6.49
PYTHON_VER=3.3
ENV_DIR=py${PYTHON_VER}

pyvenv-${PYTHON_VER} ${ENV_DIR}
source ${ENV_DIR}/bin/activate
curl -l -O "https://pypi.python.org/packages/source/d/distribute/distribute-${DISTRIBTE_VER}.tar.gz"
tar xf distribute-${DISTRIBTE_VER}.tar.gz
cd distribute-${DISTRIBTE_VER}
../${ENV_DIR}/bin/python setup.py install
cd ..
./${ENV_DIR}/bin/easy_install-3.3 pip


