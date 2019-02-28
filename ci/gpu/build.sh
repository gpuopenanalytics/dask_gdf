#!/bin/bash
# Copyright (c) 2018, NVIDIA CORPORATION.
#########################################
# cuDF GPU build and test script for CI #
#########################################
set -e

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

# Set path and build parallel level
export PATH=/conda/bin:/usr/local/cuda/bin:$PATH
export PARALLEL_LEVEL=4

# Set home to the job's workspace
export HOME=$WORKSPACE

################################################################################
# SETUP - Check environment
################################################################################

logger "Check environment..."
env

logger "Check GPU usage..."
nvidia-smi

logger "Activate conda env..."
source activate gdf

logger "Check versions..."
python --version
$CC --version
$CXX --version
conda list

################################################################################
# BUILD - Build libcudf and cuDF from source
################################################################################
git clone git@github.com:rapidsai/cudf.git CUDF-SRC

CUDF=$WORKSPACE/CUDF-SRC

logger "Build libcudf..."
mkdir -p $CUDF/cpp/build
cd $CUDF/cpp/build
logger "Run cmake libcudf..."
cmake -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX -DCMAKE_CXX11_ABI=ON ..

logger "Clean up make..."
make clean

logger "Make libcudf..."
make -j${PARALLEL_LEVEL}

logger "Install libcudf..."
make -j${PARALLEL_LEVEL} install

logger "Install libcudf for Python..."
make python_cffi
make install_python

logger "Build cuDF..."
cd $CUDF/python
python setup.py build_ext --inplace

# Temporarily install feather for testing
logger "conda install feather-format"
conda install -c conda-forge -y feather-format

# Temporarily install tzdata otherwise pyarrow core dumps
logger "apt-get update && apt-get install -y tzdata"
apt-get update && apt-get install -y tzdata

logger "Python py.test for dask-cudf..."
cd $WORKSPACE
py.test --cache-clear --junitxml=${WORKSPACE}/junit-dask-cudf.xml -v
