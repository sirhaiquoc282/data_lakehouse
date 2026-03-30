FROM apache/ranger:2.4.0

USER root

# Install PostgreSQL JDBC driver (từ Maven Central - không bị block)
RUN curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    -o /opt/ranger/ews/webapp/WEB-INF/lib/postgresql-42.7.3.jar

COPY conf/install.properties /opt/ranger/admin/install.properties
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 6080
ENTRYPOINT ["/entrypoint.sh"]