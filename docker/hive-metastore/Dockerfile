FROM apache/hive:3.1.3

USER root

# Install tools
RUN apt-get update && apt-get install -y wget curl netcat procps && rm -rf /var/lib/apt/lists/*

# Download Postgres JDBC Driver
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /opt/hive/lib/postgresql-42.6.0.jar

# Download AWS Jars for S3 Support (Apache Hive 3.1.3 ships with Hadoop 3.1.0)
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar -O /opt/hive/lib/hadoop-aws-3.1.0.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar -O /opt/hive/lib/aws-java-sdk-bundle-1.11.271.jar

# Setup configuration and startup script
COPY docker/hive-metastore/hive-site.xml /opt/hive/conf/hive-site.xml
COPY docker/hive-metastore/start.sh /opt/start.sh
RUN chmod +x /opt/start.sh

# Change ownership to hive user
RUN chown -R hive:hive /opt/hive /opt/start.sh

USER hive
WORKDIR /opt

EXPOSE 9083

ENTRYPOINT ["/opt/start.sh"]
