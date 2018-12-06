package com.datamantra.loganalysis.spark



import com.datamantra.loganalysis.Config
import com.datastax.spark.connector.cql.CassandraConnector
import com.twitter.bijection.avro.GenericAvroCodecs


import org.apache.log4j.Logger


import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
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


    val countryCodeBrodcast = sparkSession.sparkContext.broadcast(SparkUtils.createCountryCodeMap(Config.setting.sparkSettings.ipLookupFile))

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
        logger.info("fromOffset: " + offRange.fromOffset + "untill Offset: " + offRange.untilOffset)
      })

      val ecommerceValueRdd = rdd.map(_.value())

      /* Kafka sends Array of bytes to Spark Streaming consumer. Here we are converting Array of bytes to Avro Generic Record */
      val genericAvroRdd = ecommerceValueRdd.mapPartitions(partitionItr => {
        val parser = new Schema.Parser
        val ecommerceAvroSchema: Schema = parser.parse(ecommerceSchemaString)
        val recordInjection = GenericAvroCodecs.toBinary[GenericRecord](ecommerceAvroSchema);

        partitionItr.map(avroBytes => {
          recordInjection.invert(avroBytes).get
        })
      })

      /*Page views analytics */
      val pageViewsRdd = genericAvroRdd.map(avroRecord => avroRecord.get("requestUri").toString)
        .filter(_.length != 0)
        .map(requestURI => {
          logger.debug("requestURI: " + requestURI)
          val productURI = requestURI.split(" ")(1)
          val product = productURI.split("/")(3)
          (product, 1)
        }).reduceByKey(_ + _)

      pageViewsRdd.foreach(pageCount => {
        logger.debug("page: " + pageCount._1 + " count: " + pageCount._2)
        connector.withSessionDo(session => {
          session.execute(s"update ecommercelog.page_views set count = count + ${pageCount._2} where page = '${pageCount._1}'")
        })
      })


      /*Response Code Analytics */
      val statusCodeCountRdd = genericAvroRdd.map(avroRecord => {
        val status = avroRecord.get("status").toString
       logger.debug("status_code: " + status)
        (status, 1)
      }).reduceByKey(_ + _)

      statusCodeCountRdd.foreach( statusCodeCount => {
        logger.debug("statusCode: " + statusCodeCount._1 + " count: " + statusCodeCount._2)
        connector.withSessionDo(session => {
          session.execute(s"update ecommercelog.status_counter set count = count + ${statusCodeCount._2} where status_code = '${statusCodeCount._1}'")
        })
      })


      /* Referrer Analytics */
      val referrerRdd = genericAvroRdd.map(avroRecord => {
        val referrer = avroRecord.get("referrer").toString
        logger.debug("referrer: " + referrer)
        (referrer, 1)
      }).reduceByKey(_ + _)

      referrerRdd.foreach( referrerCount => {
        logger.debug("referrer: " + referrerCount._1 + " count: " + referrerCount._2)
        connector.withSessionDo(session => {
          session.execute(s"update ecommercelog.referrer_counter set count = count + ${referrerCount._2} where referrer = '${referrerCount._1}'")
        })
      })


      /* Country Visit Analytics */
      val countryVisitRdd = genericAvroRdd.map(avroRecord => avroRecord.get("ipv4").toString)
        .filter(_.length != 0)
        .map(ipaddress => {
          logger.debug("ipaddress: " + ipaddress)
          val countryCodeMap =  countryCodeBrodcast.value
          val octets = ipaddress.toString.split("\\.")
          val key = octets(0) + "." + octets(1)
          val countryCode = countryCodeMap.getOrElse(key, "unknown country")
          (countryCode, 1)
        }).reduceByKey(_ + _)

      countryVisitRdd.foreach( countryVisitCount => {
        logger.debug("country: " + countryVisitCount._1 + " count: " + countryVisitCount._2)
        connector.withSessionDo(session => {
          session.execute(s"update ecommercelog.visits_by_country set count = count + ${countryVisitCount._2} where country = '${countryVisitCount._1}'")
        })
      })

      /* After all processing is done, offset is committed to Kafka */
      kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()

  }

}
