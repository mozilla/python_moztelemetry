FROM java:8

# Versions of spark + hbase to use for our testing environment
ENV SPARK_VERSION=2.0.2
ENV HBASE_VERSION=1.2.3

# install gcc
RUN apt-get update && apt-get install -y g++ libpython-dev libsnappy-dev

# setup conda environment

# temporary workaround, pin miniconda version until it's fixed.
RUN wget https://repo.continuum.io/miniconda/Miniconda2-4.3.21-Linux-x86_64.sh -O miniconda.sh
RUN bash miniconda.sh -b -p /miniconda
ENV PATH="/miniconda/bin:${PATH}"
RUN hash -r
RUN conda config --set always_yes yes --set changeps1 no
# TODO: uncomment
# RUN conda update -q conda
RUN conda info -a # Useful for debugging any issues with conda

# install spark/hbase
RUN wget -nv https://archive.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz
RUN tar -zxf hbase-$HBASE_VERSION-bin.tar.gz

# build + activate conda environment
COPY ./environment.yml /python_moztelemetry/
RUN conda env create -f /python_moztelemetry/environment.yml

# this is roughly equivalent to activating the conda environment
ENV PATH="/miniconda/envs/test-environment/bin:${PATH}"

WORKDIR /python_moztelemetry

# we need to explicitly install pytest and dependencies so spark
# can pick them up
RUN pip install 'pytest>=3' coverage coveralls

# This will invalidate the cache if something changes in python_moztelemetry.
COPY . /python_moztelemetry

# install moztelemetry specific deps into conda env
RUN pip install /python_moztelemetry/ --process-dependency-links
