FROM ubuntu:trusty

# conda
RUN apt-get update && apt-get install -y -q curl bzip2 git
RUN curl https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
RUN /bin/bash /tmp/miniconda.sh -b -p /opt/conda
RUN rm /tmp/miniconda.sh
ENV PATH /opt/conda/bin:$PATH

# hdfs3 - python 2
RUN apt-get install -y -q protobuf-compiler libprotobuf-dev
ENV LIBHDFS3_CONF /etc/hadoop/conf/hdfs-site.xml
ENV CONDA /opt/conda/bin/conda
ENV CONDA_ENV root
ENV PIP /opt/conda/bin/pip
RUN $CONDA install -n $CONDA_ENV -y libxml2 krb5 boost ipython pytest pip pandas cython mock dask
RUN $CONDA install -n $CONDA_ENV -y libhdfs3 libgsasl libntlm -c dask
RUN $CONDA install -n $CONDA_ENV -y python-lmdb -c conda-forge
RUN $PIP install git+https://github.com/dask/hdfs3 --upgrade
RUN $PIP install git+https://github.com/dask/s3fs --upgrade
RUN $PIP install git+https://github.com/dask/zict --upgrade
RUN $PIP install git+https://github.com/dask/dask --upgrade
RUN $PIP install pytest-timeout pytest-faulthandler
# hdfs3 - python 3
RUN conda create -n py3 -y python=3
ENV CONDA_ENV py3
ENV PIP /opt/conda/envs/py3/bin/pip
# (note the copy-paste job here)
RUN $CONDA install -n $CONDA_ENV -y libxml2 krb5 boost ipython pytest pip pandas cython mock dask
RUN $CONDA install -n $CONDA_ENV -y libhdfs3 libgsasl libntlm -c dask
RUN $CONDA install -n $CONDA_ENV -y python-lmdb -c conda-forge
RUN $PIP install git+https://github.com/dask/hdfs3 --upgrade
RUN $PIP install git+https://github.com/dask/s3fs --upgrade
RUN $PIP install git+https://github.com/dask/zict --upgrade
RUN $PIP install git+https://github.com/dask/dask --upgrade
RUN $PIP install pytest-timeout pytest-faulthandler

# Cloudera repositories
RUN curl -s http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh/archive.key | apt-key add -
RUN echo 'deb [arch=amd64] http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' > /etc/apt/sources.list.d/cloudera.list
RUN echo 'deb-src http://archive.cloudera.com/cdh5/ubuntu/trusty/amd64/cdh trusty-cdh5 contrib' >> /etc/apt/sources.list.d/cloudera.list
ADD docker-files/cloudera.pref /etc/apt/preferences.d/cloudera.pref

# Install CDH5 in a single node: Pseudo Distributed
ADD docker-files/cdh5-install.sh /tmp/cdh5-install.sh
RUN bash /tmp/cdh5-install.sh

EXPOSE 8020
EXPOSE 50070

VOLUME /distributed
WORKDIR /distributed

ADD docker-files/start.sh /tmp/start.sh
CMD ["bash", "/tmp/start.sh"]
