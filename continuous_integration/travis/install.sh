#
# This file should be source'd, so as to update the caller's environment
# (such as the PATH variable)
#

# Note we disable progress bars to make Travis log loading much faster

# Set default variable values if unset
# (useful when this script is not invoked by Travis)
: ${PYTHON:=3.8}
: ${TORNADO:=6}
: ${PACKAGES:=python-snappy python-blosc}

# Install conda
case "$(uname -s)" in
    'Darwin')
        MINICONDA_FILENAME="Miniconda3-latest-MacOSX-x86_64.sh"
        ;;
    'Linux')
        MINICONDA_FILENAME="Miniconda3-latest-Linux-x86_64.sh"
        ;;
    *)  ;;
esac

if ! which conda; then
  wget https://repo.continuum.io/miniconda/$MINICONDA_FILENAME -O miniconda.sh
  bash miniconda.sh -b -p $HOME/miniconda
  export PATH="$HOME/miniconda/bin:$PATH"
fi

conda config --set always_yes yes --set quiet yes --set changeps1 no
conda update conda

# Create conda environment
conda create -n dask-distributed -c pkgs/main python=$PYTHON
source activate dask-distributed

# Install dependencies
conda install -c conda-forge \
    asyncssh \
    bokeh \
    click \
    coverage \
    dask \
    flake8 \
    h5py \
    ipykernel \
    ipywidgets \
    joblib \
    jupyter_client \
    msgpack-python \
    netcdf4 \
    paramiko \
    prometheus_client \
    psutil \
    pytest \
    pytest-timeout \
    requests \
    scikit-learn \
    scipy \
    tblib \
    toolz \
    tornado=$TORNADO \
    zstandard \
    $PACKAGES

# stacktrace is not currently avaiable for Python 3.8.
# Remove the version check block below when it is avaiable.
if [[ $PYTHON != 3.8 ]]; then
    # For low-level profiler, install libunwind and stacktrace from conda-forge
    # For stacktrace we use --no-deps to avoid upgrade of python
    conda install -c pkgs/main -c conda-forge libunwind
    conda install --no-deps -c pkgs/main -c numba -c conda-forge stacktrace
fi

python -m pip install -q "pytest>=4" pytest-repeat pytest-faulthandler pytest-asyncio
python -m pip install -q git+https://github.com/dask/dask.git --upgrade --no-deps
python -m pip install -q git+https://github.com/joblib/joblib.git --upgrade --no-deps
python -m pip install -q git+https://github.com/intake/filesystem_spec.git --upgrade --no-deps
python -m pip install -q git+https://github.com/dask/s3fs.git --upgrade --no-deps
python -m pip install -q git+https://github.com/dask/zict.git --upgrade --no-deps
python -m pip install -q sortedcollections --no-deps
python -m pip install -q keras --upgrade --no-deps

if [[ $CRICK == true ]]; then
    conda install -c pkgs/main cython
    python -m pip install -q git+https://github.com/jcrist/crick.git
fi

# Install distributed
python -m pip install --no-deps -e .

# For debugging
echo -e "--\n--Conda Environment\n--"
conda list

echo -e "--\n--Pip Environment\n--"
python -m pip list --format=columns
