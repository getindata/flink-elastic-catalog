FROM jupyter/minimal-notebook:python-3.8.8
ARG FLINK_VERSION
ARG ELASTIC_VERSION

USER root
RUN apt-get update && apt-get install -y software-properties-common curl wget telnet
RUN apt-add-repository ppa:openjdk-r/ppa
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk-headless
    
# Set user back to priviledged user.
USER $NB_USER

# Install python dependencies
RUN python3 -m pip install pandas==1.3.5
RUN python3 -m pip install jupyterlab-lsp \
    streaming-jupyter-integrations==0.13.1 \
    apache-flink==${FLINK_VERSION} \
    apache-flink-libraries==${FLINK_VERSION} \
    --use-deprecated=legacy-resolver
RUN python3 -m pip install --upgrade numpy
RUN python3 -m pip install ipytree

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PYFLINK_LIB_DIR /opt/conda/lib/python3.8/site-packages/pyflink/lib

# Copy Jupyter configuration
COPY example/docker/jupyter/jupyter_notebook_config.py /home/jovyan/.jupyter/
COPY example/docker/jupyter/jupyter_server_config.py /home/jovyan/.jupyter/

# Copy/download Flink extra dependencies
COPY flink-connector-jdbc/target/flink-connector-jdbc2-${FLINK_VERSION}.jar $PYFLINK_LIB_DIR
COPY target/flink-elastic-catalog-1.0-SNAPSHOT.jar $PYFLINK_LIB_DIR
RUN wget https://repo1.maven.org/maven2/org/elasticsearch/plugin/x-pack-sql-jdbc/${ELASTIC_VERSION}/x-pack-sql-jdbc-${ELASTIC_VERSION}.jar \
    -O $PYFLINK_LIB_DIR/x-pack-sql-jdbc-${ELASTIC_VERSION}.jar
