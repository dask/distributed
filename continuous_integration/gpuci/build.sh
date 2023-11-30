##############################################
# Dask GPU build and test script for CI      #
##############################################
set -e
NUMARGS=$#
ARGS=$*

# Arg parsing function
function hasArg {
    (( ${NUMARGS} != 0 )) && (echo " ${ARGS} " | grep -q " $1 ")
}

# Set path and build parallel level
export PATH=/opt/conda/bin:/usr/local/cuda/bin:$PATH
export PARALLEL_LEVEL=${PARALLEL_LEVEL:-4}

# Set home to the job's workspace
export HOME="$WORKSPACE"

# Switch to project root; also root of repo checkout
cd "$WORKSPACE"

# Determine CUDA release version
export CUDA_REL=${CUDA_VERSION%.*}

# FIXME - monitoring GIL contention causes UCX teardown issues
export DASK_DISTRIBUTED__ADMIN__SYSTEM_MONITOR__GIL__ENABLED=False

################################################################################
# SETUP - Check environment
################################################################################

rapids-logger "Check environment variables"
env

rapids-logger "Check GPU usage"
nvidia-smi

rapids-logger "Activate conda env"
. /opt/conda/etc/profile.d/conda.sh
conda activate dask

rapids-logger "Install distributed"
python -m pip install -e .

rapids-logger "Install dask"
python -m pip install git+https://github.com/dask/dask

rapids-logger "Check Python versions"
python --version

rapids-logger "Check conda environment"
conda info
conda config --show-sources
conda list --show-channel-urls

rapids-logger "Python py.test for distributed"
py.test distributed -v -m gpu --runslow --junitxml="$WORKSPACE/junit-distributed.xml"
