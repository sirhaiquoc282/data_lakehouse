FROM confluentinc/cp-kafka-connect:7.7.8

USER root

RUN microdnf update -y && \
    microdnf install -y curl unzip && \
    microdnf clean all

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:12.1.0
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6

RUN mkdir -p /usr/share/java/debezium


RUN curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.5.0.Final/debezium-connector-mysql-3.5.0.Final-plugin.tar.gz \
    -o /tmp/mysql.tar.gz && \
    mkdir -p /usr/share/java/debezium/mysql && \
    tar -xzf /tmp/mysql.tar.gz -C /usr/share/java/debezium/mysql --strip-components=1 && \
    rm /tmp/mysql.tar.gz

RUN curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.5.0.Final/debezium-connector-postgres-3.5.0.Final-plugin.tar.gz \
    -o /tmp/postgres.tar.gz && \
    mkdir -p /usr/share/java/debezium/postgres && \
    tar -xzf /tmp/postgres.tar.gz -C /usr/share/java/debezium/postgres --strip-components=1 && \
    rm /tmp/postgres.tar.gz

RUN curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/3.5.0.Final/debezium-connector-oracle-3.5.0.Final-plugin.tar.gz \
    -o /tmp/oracle.tar.gz && \
    mkdir -p /usr/share/java/debezium/oracle && \
    tar -xzf /tmp/oracle.tar.gz -C /usr/share/java/debezium/oracle --strip-components=1 && \
    rm /tmp/oracle.tar.gz

RUN curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-mongodb/3.5.0.Final/debezium-connector-mongodb-3.5.0.Final-plugin.tar.gz \
    -o /tmp/mongodb.tar.gz && \
    mkdir -p /usr/share/java/debezium/mongodb && \
    tar -xzf /tmp/mongodb.tar.gz -C /usr/share/java/debezium/mongodb --strip-components=1 && \
    rm /tmp/mongodb.tar.gz

RUN curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-sqlserver/3.5.0.Final/debezium-connector-sqlserver-3.5.0.Final-plugin.tar.gz \
    -o /tmp/sqlserver.tar.gz && \
    mkdir -p /usr/share/java/debezium/sqlserver && \
    tar -xzf /tmp/sqlserver.tar.gz -C /usr/share/java/debezium/sqlserver --strip-components=1 && \
    rm /tmp/sqlserver.tar.gz

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

USER appuser