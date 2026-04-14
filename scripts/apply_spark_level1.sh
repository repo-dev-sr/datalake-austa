#!/usr/bin/env bash
set -euo pipefail

# ── spark-defaults.conf ────────────────────────────────────────────
cat > /opt/spark/conf/spark-defaults.conf << 'SPARK_EOF'
spark.sql.extensions                                   org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.defaultCatalog                               glue_catalog
spark.sql.catalog.glue_catalog                         org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.catalog-impl            org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl                 org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.glue_catalog.warehouse               s3a://austa-lakehouse-prod-data-lake-169446931765/
spark.sql.catalog.glue_catalog.client.region           sa-east-1
spark.hadoop.fs.s3a.aws.credentials.provider           com.amazonaws.auth.InstanceProfileCredentialsProvider
spark.hadoop.fs.s3a.path.style.access                  false
spark.sql.warehouse.dir                                s3a://austa-lakehouse-prod-data-lake-169446931765/
spark.sql.catalog.glue_catalog.cache-enabled           true
spark.sql.catalog.glue_catalog.cache.expiration-interval-ms  300000

spark.master                              local[4]
spark.driver.memory                       8g
spark.executor.memory                     4g
spark.sql.adaptive.enabled                true
spark.sql.shuffle.partitions              8
spark.driver.extraJavaOptions             -XX:MetaspaceSize=256m -Djava.io.tmpdir=/var/lib/spark/tmp
spark.executor.extraJavaOptions           -XX:MetaspaceSize=256m -Djava.io.tmpdir=/var/lib/spark/tmp
spark.local.dir                           file:///var/lib/spark/local

# ── NIVEL 1: Vectorized Reader ──
spark.sql.parquet.enableVectorizedReader  true

# ── NIVEL 1: Small Files Coalescing ──
spark.sql.files.openCostInBytes           1048576
spark.sql.files.maxPartitionBytes         268435456

# ── NIVEL 1: S3A Performance ──
spark.hadoop.fs.s3a.connection.maximum    200
spark.hadoop.fs.s3a.threads.max           200
spark.hadoop.fs.s3a.readahead.range       6291456
spark.hadoop.fs.s3a.fast.upload           true
spark.hadoop.fs.s3a.multipart.size        67108864
spark.hadoop.fs.s3a.block.size            67108864
spark.hadoop.fs.s3a.experimental.input.fadvise  random
SPARK_EOF

echo "spark-defaults.conf atualizado."

# ── kyuubi-defaults.conf ───────────────────────────────────────────
cat > /opt/kyuubi/conf/kyuubi-defaults.conf << 'KYUUBI_EOF'
kyuubi.frontend.bind.host                                      0.0.0.0
kyuubi.frontend.protocols                                      THRIFT_BINARY,REST
kyuubi.frontend.thrift.binary.port                             10009
kyuubi.frontend.rest.bind.host                                  0.0.0.0
kyuubi.frontend.rest.bind.port                                 10099
kyuubi.frontend.rest.ui.enabled                                 true

kyuubi.authentication                                         LDAP
kyuubi.authentication.ldap.url                                 ldap://127.0.0.1:389
kyuubi.authentication.ldap.baseDN                              dc=austa,dc=local
kyuubi.authentication.ldap.userDNPattern                       uid=%s,ou=people,dc=austa,dc=local

kyuubi.engine.type                                             SPARK_SQL
kyuubi.engine.spark.master                                     spark://172.36.2.222:7077
kyuubi.engine.share.level                                      USER
kyuubi.engine.spark.driver.memory                               2g

kyuubi.session.engine.idle.timeout                              PT2H
kyuubi.session.idle.timeout                                    PT6H
kyuubi.operation.idle.timeout                                  PT3H

kyuubi.engine.spark.conf.spark.sql.extensions                  org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
kyuubi.engine.spark.conf.spark.sql.defaultCatalog              glue_catalog
kyuubi.engine.spark.conf.spark.sql.catalogImplementation      in-memory
kyuubi.engine.spark.conf.spark.sql.catalog.glue_catalog        org.apache.iceberg.spark.SparkCatalog
kyuubi.engine.spark.conf.spark.sql.catalog.glue_catalog.catalog-impl    org.apache.iceberg.aws.glue.GlueCatalog
kyuubi.engine.spark.conf.spark.sql.catalog.glue_catalog.io-impl org.apache.iceberg.aws.s3.S3FileIO
kyuubi.engine.spark.conf.spark.sql.catalog.glue_catalog.warehouse       s3a://austa-lakehouse-prod-data-lake-169446931765/
kyuubi.engine.spark.conf.spark.sql.catalog.glue_catalog.client.region   sa-east-1
kyuubi.engine.spark.conf.spark.sql.iceberg.handle-timestamp-without-timezone  true
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.aws.credentials.provider   com.amazonaws.auth.InstanceProfileCredentialsProvider
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.path.style.access false
kyuubi.engine.spark.conf.spark.sql.catalog.glue_catalog.cache-enabled  true
kyuubi.engine.spark.conf.spark.sql.catalog.glue_catalog.cache.expiration-interval-ms  300000

kyuubi.engine.spark.conf.spark.master                          local[*]
kyuubi.engine.spark.conf.spark.driver.memory                   8g
kyuubi.engine.spark.conf.spark.executor.memory                 4g
kyuubi.engine.spark.conf.spark.executor.cores                  2
kyuubi.engine.spark.conf.spark.sql.adaptive.enabled            true
kyuubi.engine.spark.conf.spark.sql.adaptive.coalescePartitions.enabled  true
kyuubi.engine.spark.conf.spark.sql.adaptive.skewJoin.enabled   true
kyuubi.engine.spark.conf.spark.sql.shuffle.partitions          8
kyuubi.engine.spark.conf.spark.driver.extraJavaOptions         -XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=768m -Djava.io.tmpdir=/var/lib/spark/tmp
kyuubi.engine.spark.conf.spark.executor.extraJavaOptions       -XX:MetaspaceSize=512m -XX:MaxMetaspaceSize=768m -Djava.io.tmpdir=/var/lib/spark/tmp
kyuubi.engine.spark.conf.spark.local.dir                       file:///var/lib/spark/local

# ── NIVEL 1: Vectorized Reader ──
kyuubi.engine.spark.conf.spark.sql.parquet.enableVectorizedReader  true

# ── NIVEL 1: Small Files Coalescing ──
kyuubi.engine.spark.conf.spark.sql.files.openCostInBytes       1048576
kyuubi.engine.spark.conf.spark.sql.files.maxPartitionBytes     268435456

# ── NIVEL 1: S3A Performance ──
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.connection.maximum     200
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.threads.max            200
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.readahead.range        6291456
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.fast.upload            true
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.multipart.size         67108864
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.block.size             67108864
kyuubi.engine.spark.conf.spark.hadoop.fs.s3a.experimental.input.fadvise  random
KYUUBI_EOF

echo "kyuubi-defaults.conf atualizado."
echo "DONE"
