FROM apache/spark:4.1.1-scala2.13-java21-python3-r-ubuntu

USER root

RUN pip install --no-cache-dir delta-spark==4.1.0

RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
        -o /opt/spark/jars/hadoop-aws-3.3.6.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
        -o /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar

USER spark