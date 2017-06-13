#
# This file should be source'd, so as to update the caller's environment
# (such as the PATH variable)
#

# Note we disable progress bars to make Travis log loading much faster

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

wget https://repo.continuum.io/miniconda/$MINICONDA_FILENAME -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
conda config --set always_yes yes --set changeps1 no
conda update -q conda

# Create conda environment
conda create -q -n test-environment python=$PYTHON
source activate test-environment

# Install dependencies
# (Tornado pinned to 4.4 until a new Bokeh is released)
conda install -q -c conda-forge \
    bokeh \
    click \
    coverage \
    dask \
    dill \
    flake8 \
    h5py \
    ipykernel \
    ipywidgets \
    joblib \
    jupyter_client \
    lz4 \
    mock \
    netcdf4 \
    paramiko \
    psutil \
    pytest \
    pytest-faulthandler \
    pytest-timeout \
    requests \
    tblib \
    toolz \
    tornado=4.4 \
    $PACKAGES

if [[ $HDFS == true ]]; then
    conda install -q libxml2 krb5 boost
    conda install -q -c conda-forge libhdfs3 libgsasl libntlm
    pip install -q git+https://github.com/dask/hdfs3 --upgrade
fi;

pip install -q git+https://github.com/dask/dask.git --upgrade
pip install -q git+https://github.com/joblib/joblib.git --upgrade
pip install -q git+https://github.com/dask/s3fs.git --upgrade
pip install -q git+https://github.com/dask/zict.git --upgrade
pip install -q sortedcollections msgpack-python
pip install -q keras --upgrade --no-deps

if [[ $CRICK == true ]]; then
    conda install -q cython
    pip install -q git+https://github.com/jcrist/crick.git
fi;

# Install distributed
pip install --no-deps -e .

# For debugging
echo -e "--\n--Conda Environment\n--"
conda list

echo -e "--\n--Pip Environment\n--"
pip list --format=columns
