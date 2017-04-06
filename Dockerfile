FROM java:8

# Versions of spark + hbase to use for our testing environment
ENV SPARK_VERSION=2.0.2
ENV HBASE_VERSION=1.2.3

# install gcc
RUN apt-get update && apt-get install -y g++ libpython-dev libsnappy-dev

# setup conda environment
RUN wget https://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh -O miniconda.sh
RUN bash miniconda.sh -b -p /miniconda
ENV PATH="/miniconda/bin:${PATH}"
RUN hash -r
RUN conda config --set always_yes yes --set changeps1 no
RUN conda update -q conda
RUN conda info -a # Useful for debugging any issues with conda

# install spark/hbase
ADD https://d3kbcqa49mib13.cloudfront.net/spark-$SPARK_VERSION-bin-hadoop2.6.tgz /
ADD https://archive.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz /
RUN tar -zxf spark-$SPARK_VERSION-bin-hadoop2.6.tgz
RUN tar -zxf hbase-$HBASE_VERSION-bin.tar.gz
ENV SPARK_HOME="/spark-${SPARK_VERSION}-bin-hadoop2.6"

COPY . /python_moztelemetry

# build + activate conda environment
RUN conda env create -f /python_moztelemetry/environment.yml
RUN bash -c 'source activate test-environment'

# install moztelemetry specific deps into conda env
RUN pip install /python_moztelemetry/ --process-dependency-links
# we need to explicitly install pytest and dependencies so spark
# can pick them up
RUN pip install 'pytest>=3' coverage coveralls

WORKDIR /python_moztelemetry
