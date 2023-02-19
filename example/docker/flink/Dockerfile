ARG FLINK_VERSION
FROM flink:${FLINK_VERSION}-scala_2.12-java8
ARG FLINK_VERSION
ARG ELASTIC_VERSION

# Install Python
# https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/resource-providers/standalone/docker/#using-flink-python-on-docker
RUN apt-get update -y && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
    wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
    tar -xvf Python-3.7.9.tgz && \
    cd Python-3.7.9 && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-3.7.9.tgz && rm -rf Python-3.7.9 && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and other necessary dependencies.
RUN python3 -m pip install \
    apache-flink==${FLINK_VERSION} \
    apache-flink-libraries==${FLINK_VERSION}

# Upgrade conflicting dependency.
# FIXME
RUN python3 -m pip install typing-extensions --upgrade || true

ENV FLINK_LIB_DIR /opt/flink/lib

# Copy Flink dependencies
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.0.0-1.16/flink-connector-jdbc-3.0.0-1.16.jar \
    -O ${FLINK_LIB_DIR}/flink-connector-jdbc-3.0.0-1.16.jar

# Copy elastic dependencies for Flink
RUN wget https://repo1.maven.org/maven2/org/elasticsearch/plugin/x-pack-sql-jdbc/${ELASTIC_VERSION}/x-pack-sql-jdbc-${ELASTIC_VERSION}.jar \
    -O ${FLINK_LIB_DIR}/x-pack-sql-jdbc-${ELASTIC_VERSION}.jar
COPY flink-elastic-catalog/target/flink-elastic-catalog-*.jar ${FLINK_LIB_DIR}
COPY flink-elastic-dialect/target/flink-elastic-dialect-*.jar ${FLINK_LIB_DIR}

