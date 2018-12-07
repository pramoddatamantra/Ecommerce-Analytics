package com.datamantra.loganalysis

import com.datamantra.loganalysis.cassandra.CassandraSettings
import com.datamantra.loganalysis.kafka.KafkaSettings
import com.datamantra.loganalysis.spark.SparkSettings
import com.typesafe.config.Config
import org.apache.log4j.Logger

/**
 * Created by kafka on 15/11/18.
 */
class Settings(config: Config) {

  val logger = Logger.getLogger(getClass.getName)

  val cassandraSettings = CassandraSettings(
    config.getString("cassandra.hostname"),
    config.getString("cassandra.keyspace"),
    config.getString("cassandra.table.page.views"),
    config.getString("cassandra.table.status.counter"),
    config.getString("cassandra.table.visit.by.country"),
    config.getString("cassandra.table.referrer.counter"),
    config.getString("cassandra.output.batch.size.bytes")
  )
  val kafkaSettings = KafkaSettings(
    config.getString("kafka.bootstrap.server"),
    config.getString("kafka.topic"),
    config.getString("kafka.group.id"),
    config.getString("kafka.key.deserializer"),
    config.getString("kafka.value.deserializer"),
    config.getString("kafka.schema.registry")
  )
  val sparkSettings = SparkSettings(
    config.getString("spark.master"),
    config.getString("spark.name"),
    config.getString("spark.batch.interval"),
    config.getString("spark.window.interval").toInt,
    config.getString("spark.max.rate.per.partition"),
    config.getString("spark.ip.lookup.file")

  )
}
