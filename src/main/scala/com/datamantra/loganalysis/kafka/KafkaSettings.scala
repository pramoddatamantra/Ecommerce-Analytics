package com.datamantra.loganalysis.kafka

/**
 * Created by kafka on 15/11/18.
 */
case class KafkaSettings(
  bootstrapServer:String,
  topic:String,
  groupId:String,
  keyDeserializer:String,
  valueDeserializer:String,
  schemaRegistry:String
)
