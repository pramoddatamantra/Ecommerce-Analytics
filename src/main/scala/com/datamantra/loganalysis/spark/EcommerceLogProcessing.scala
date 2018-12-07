package com.datamantra.loganalysis.spark



import com.datamantra.loganalysis.Config
import com.datamantra.loganalysis.cassandra.CassandraUtils
import com.datastax.spark.connector.cql.CassandraConnector


import org.apache.log4j.Logger


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, CanCommitOffsets, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies._



/**
 * Created by kafka on 16/11/18.
 */
object EcommerceLogProcessing {


  val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    Config.parseArgs(args)

    val conf = new SparkConf()
    .setMaster(Config.setting.sparkSettings.master)
    .setAppName(Config.setting.sparkSettings.name)
    .set("spark.streaming.kafka.maxRatePerPartition", Config.setting.sparkSettings.maxRatePerPartition)
    .set("spark.cassandra.connection.host", Config.setting.cassandraSettings.hostname)



    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.setting.kafkaSettings.bootstrapServer,
      ConsumerConfig.GROUP_ID_CONFIG -> Config.setting.kafkaSettings.groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        Config.setting.kafkaSettings.keyDeserializer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        Config.setting.kafkaSettings.valueDeserializer
    )


    val sparkSession =  SparkSession.builder
      .config(conf)
      .getOrCreate()

    val batchInterval = Config.setting.sparkSettings.batchInterval.toLong
    val topics = Set(Config.setting.kafkaSettings.topic)

    /* Here we are connecting to Schema Registry through REST and getting latest schema for the given topic */
    val ecommerceSchemaString = SparkUtils.getSchema(Config.setting.kafkaSettings.schemaRegistry, Config.setting.kafkaSettings.topic)


    val countryCodeBroadcast = sparkSession.sparkContext.broadcast(SparkUtils.createCountryCodeMap(Config.setting.sparkSettings.ipLookupFile))

    /*
       Connector Object is created in driver. It is serializable.
       So once the executor get it, they establish the real connection
    */

    val connector = CassandraConnector(sparkSession.sparkContext.getConf)

    val ssc = new StreamingContext(sparkSession.sparkContext, Duration(batchInterval))


    logger.info("Connecting to Kafka")
    val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte]](ssc,
                       PreferConsistent,
                       Subscribe[String, Array[Byte]](topics, kafkaParams)
                       )

    kafkaStream.foreachRDD(rdd => {

      if(rdd.isEmpty()) {
        logger.info("Did not receive any data")
      }

      /* Extract Offset */
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(offRange => {
        logger.debug("fromOffset: " + offRange.fromOffset + "untill Offset: " + offRange.untilOffset)
      })

      val ecommerceValueRdd = rdd.map(_.value())

      /* Kafka sends Array of bytes to Spark Streaming consumer. Here we are converting Array of bytes to Avro Generic Record */
      val genericAvroRdd = SparkUtils.bytesToAvroGenriceRecord(ecommerceValueRdd, ecommerceSchemaString)

      /*Page views analytics */
      val pageViewsRdd = SparkUtils.getPageViews(genericAvroRdd)
      CassandraUtils.updateToCassandra(pageViewsRdd, connector, Config.setting.cassandraSettings.keyspace, Config.setting.cassandraSettings.pageViewsTable, "page")

      /*Response Code Analytics */
      val statusCodeCountRdd = SparkUtils.getStatusCount(genericAvroRdd)
      CassandraUtils.updateToCassandra(statusCodeCountRdd, connector, Config.setting.cassandraSettings.keyspace, Config.setting.cassandraSettings.statusCounterTable, "status_code")

      /* Referrer Analytics */
      val referrerRdd = SparkUtils.getReferrer(genericAvroRdd)
      CassandraUtils.updateToCassandra(referrerRdd, connector, Config.setting.cassandraSettings.keyspace, Config.setting.cassandraSettings.referrerCounterTable, "referrer")

      /* Country Visit Analytics */
      val countryVisitRdd = SparkUtils.getVisitsByCounrty(genericAvroRdd, countryCodeBroadcast)
      CassandraUtils.updateToCassandra(countryVisitRdd, connector, Config.setting.cassandraSettings.keyspace, Config.setting.cassandraSettings.visitsByCountryTable, "country")

      /* After all processing is done, offset is committed to Kafka */
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
