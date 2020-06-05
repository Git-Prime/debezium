FROM debezium/connect:1.1
RUN rm /kafka/connect/debezium-connector-postgres/debezium-api-*
RUN rm /kafka/connect/debezium-connector-postgres/debezium-connector-postgres-*
RUN rm /kafka/connect/debezium-connector-postgres/debezium-core-*

COPY debezium-core/target/debezium-core-*-SNAPSHOT.jar /kafka/connect/debezium-connector-postgres
COPY debezium-api/target/debezium-api-*-SNAPSHOT.jar /kafka/connect/debezium-connector-postgres
COPY debezium-connector-postgres/target/debezium-connector-postgres-*-SNAPSHOT.jar /kafka/connect/debezium-connector-postgres