FROM apache/kafka:3.9.0

USER root
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar /opt/jmx/jmx_prometheus_javaagent.jar
COPY jmx-exporter.yml /opt/jmx/jmx-exporter.yml
ENV KAFKA_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent.jar=7071:/opt/jmx/jmx-exporter.yml"
