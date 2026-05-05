FROM apache/ranger:2.8.0

USER root

RUN curl -fsSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    -o /opt/ranger/admin/ews/webapp/WEB-INF/lib/postgresql-42.7.3.jar

# COPY conf/install.properties /opt/ranger/admin/install.properties
# COPY entrypoint.sh /entrypoint.sh

# RUN chmod +x /entrypoint.sh \
#     && sed -i 's/\r$//' /entrypoint.sh

# EXPOSE 6080
# ENTRYPOINT ["/entrypoint.sh"]