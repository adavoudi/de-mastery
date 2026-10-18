# Spark 4.0.0 + the two Iceberg jars baked in.
#
# WHY a build step instead of --packages at runtime:
#   Resolving --packages makes Spark write an ivy resolution cache under
#   /opt/spark/.ivy2 at startup. On a fresh named volume that directory is not
#   writable by the image's `spark` user, so the Connect server failed to start
#   with FileNotFoundException. Baking the jars in removes all runtime network
#   + ivy resolution: the Connect server, spark-sql and pyspark all just work.
#
# The two jars are the standard Iceberg-on-Spark pair:
#   iceberg-spark-runtime-4.0_2.13 : engine integration (catalog, SQL ext, I/O)
#   iceberg-aws-bundle             : S3FileIO + bundled AWS SDK (needed for silo)
# Pins are duplicated in labs/versions.lock.
FROM apache/spark:4.0.0-python3

ARG ICEBERG_VERSION=1.10.0
ARG SPARK_ICEBERG_ARTIFACT=iceberg-spark-runtime-4.0_2.13
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fsSL -o "${SPARK_ICEBERG_ARTIFACT}-${ICEBERG_VERSION}.jar" \
      "${MAVEN}/org/apache/iceberg/${SPARK_ICEBERG_ARTIFACT}/${ICEBERG_VERSION}/${SPARK_ICEBERG_ARTIFACT}-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    ls -l iceberg-*.jar
USER spark
