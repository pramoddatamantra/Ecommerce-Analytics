package com.datamantra.loganalysis

import com.datamantra.loganalysis.cassandra.CassandraSettings
import com.datamantra.loganalysis.kafka.KafkaSettings
import com.datamantra.loganalysis.spark.SparkSettings
import com.typesafe.config.Config

/**
 * Created by kafka on 15/11/18.
 */
class Settings(config: Config) {

  val cassandraSettings = CassandraSettings(
    config.getString("loganalysis.cassandra.hostname"),
    config.getString("loganalysis.cassandra.keyspace"),
    config.getString("loganalysis.cassandra.table.page.views"),
    config.getString("loganalysis.cassandra.table.status.counter"),
    config.getString("loganalysis.cassandra.table.visit.by.country"),
    config.getString("loganalysis.cassandra.table.referrer.counter"),
    config.getString("loganalysis.cassandra.output.batch.size.bytes")
  )
  val kafkaSettings = KafkaSettings(
    config.getString("loganalysis.kafka.bootstrap.server"),
    config.getString("loganalysis.kafka.topic"),
    config.getString("loganalysis.kafka.group.id"),
    config.getString("loganalysis.kafka.key.deserializer"),
    config.getString("loganalysis.kafka.value.deserializer"),
    config.getString("loganalysis.kafka.schema.registry")
  )
  val sparkSettings = SparkSettings(
    config.getString("loganalysis.spark.master"),
    config.getString("loganalysis.spark.name"),
    config.getString("loganalysis.spark.batch.interval"),
    config.getString("loganalysis.spark.window.interval").toInt,
    config.getString("loganalysis.spark.max.rate.per.partition"),
    config.getString("loganalysis.spark.ip.lookup.file")

  )
}
