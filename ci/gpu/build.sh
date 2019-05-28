#!/bin/bash
# Copyright (c) 2019, NVIDIA CORPORATION.
##############################################
# dask-cudf GPU build and test script for CI #
##############################################
set -e

# Logger function for build status output
function logger() {
  echo -e "\n>>>> $@\n"
}

# Set path and build parallel level
export PATH=/conda/bin:/usr/local/cuda/bin:$PATH
export PARALLEL_LEVEL=4
export CUDA_REL=${CUDA_VERSION%.*}

# Set home to the job's workspace
export HOME=$WORKSPACE

# Parse git describe
export GIT_DESCRIBE_TAG=`git describe`
export MINOR_VERSION=`echo $GIT_DESCRIBE_TAG | grep -o -E '([0-9]\.[0-9])'`

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

logger "Setup new environment..."
conda install "cudf=$MINOR_VERSION.*" "dask>=1.1.5"
pip install git+https://github.com/dask/dask.git --upgrade --no-deps

conda list

logger "Python py.test for dask-cudf..."
cd $WORKSPACE
pip install -e .
py.test dask_cudf/ --cache-clear --junitxml=${WORKSPACE}/junit-dask-cudf.xml -v
