FROM quay.io/debezium/connect:2.5

USER root

# Download và cài Confluent S3 Sink Connector vào plugin path của Kafka Connect
# Archive từ Confluent Hub sẽ giải nén thành thư mục con trong /kafka/connect/
# → Kafka Connect tự scan và load plugin khi khởi động
ARG S3_CONNECTOR_VERSION=10.5.14
RUN curl -fsSL \
    "https://api.hub.confluent.io/api/plugins/confluentinc/kafka-connect-s3/versions/${S3_CONNECTOR_VERSION}/archive" \
    -o /tmp/kafka-connect-s3.zip \
    && unzip /tmp/kafka-connect-s3.zip -d /kafka/connect/ \
    && rm /tmp/kafka-connect-s3.zip

USER 1001
