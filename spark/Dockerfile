FROM apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu

USER root
RUN pip install --no-cache-dir delta-spark==3.2.0

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.603/aws-java-sdk-bundle-1.12.603.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.603.jar

RUN curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar

USER root