FROM gradle:6.6-jdk11 AS build
ARG release_version=0.0.0
ARG bintray_user
ARG bintray_key
ARG vcs_url
COPY ./ .
RUN gradle --no-daemon clean dockerPrepare \
    -Pbintray_user=${bintray_user} \
    -Pbintray_key=${bintray_key} \
    -Pvcs_url=${vcs_url}

FROM adoptopenjdk/openjdk11:alpine
ENV CRADLE_INSTANCE_NAME=instance1 \
    CASSANDRA_DATA_CENTER=kos \
    CASSANDRA_HOST=cassandra \
    CASSANDRA_PORT=9042 \
    CASSANDRA_KEYSPACE=demo \
    CASSANDRA_USERNAME=guest \
    CASSANDRA_PASSWORD=guest \
    GRPC_PORT=8080
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "/home/service/etc/config.yml"]
