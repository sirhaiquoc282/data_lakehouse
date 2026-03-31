FROM apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu

USER root

RUN apt-get update && apt-get install -y curl vim && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.603/aws-java-sdk-bundle-1.12.603.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.603.jar

RUN curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
    -o /opt/spark/jars/delta-spark_2.12-3.3.2.jar && \
    curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
    -o /opt/spark/jars/delta-storage-3.3.2.jar

ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

