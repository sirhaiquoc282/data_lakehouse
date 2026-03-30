FROM apache/hive:4.0.0

USER root

RUN apt-get update && apt-get install -y curl vim && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
        -o /opt/hive/lib/postgresql.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
        -o /opt/hive/lib/hadoop-aws.jar && \
    curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
        -o /opt/hive/lib/aws-java-sdk-bundle.jar

COPY conf/hive-site.xml /opt/hive/conf/hive-site.xml
COPY conf/core-site.xml /opt/hive/conf/core-site.xml

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

USER hive
EXPOSE 9083
ENTRYPOINT ["/entrypoint.sh"]