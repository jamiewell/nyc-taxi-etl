FROM eclipse-temurin:17-jre-jammy

# Set environment variables
ENV SPARK_VERSION=3.5.3 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

# Install Python and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    curl \
    procps \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Download and install Spark
RUN curl -fSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# Install PySpark
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION}

# Set PATH
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Create directories for logs and events
RUN mkdir -p ${SPARK_HOME}/logs ${SPARK_HOME}/work ${SPARK_HOME}/spark-events

WORKDIR ${SPARK_HOME}

# Expose ports
# 8080: Master Web UI
# 7077: Master port for worker connections
# 8081: Worker Web UI
# 4040: Application Web UI
# 18080: History Server
EXPOSE 8080 7077 8081 4040 18080

# Copy startup script
COPY start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

CMD ["/start-spark.sh"]
