package com.datamantra.loganalysis.spark


import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable


import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema


/**
 * Created by kafka on 19/11/18.
 */
object SparkUtils {

  val logger = Logger.getLogger(getClass.getName)

  def getRecordInjection() = {

    val parser = new Schema.Parser
    val avroSchema: Schema = parser.parse(getSchema)
    val recordInjection = GenericAvroCodecs.toBinary(avroSchema);
    recordInjection
  }


  def getSchema(schemaRegistryURL:String, topic:String) = {

    val restService = new RestService(schemaRegistryURL)
    val subjectValueName = topic +  "-value"
    restService.getLatestVersion(subjectValueName).getSchema
  }


  def getSchema() = {

    val schemaString = """{
                         "fields": [
                              {"name": "ipv4", "type": "string"},
                              {"name": "clientId", "type": "string"},
                              {"name": "timestamp", "type": "string"},
                              {"name": "requestUri", "type": "string"},
                              {"name": "status", "type": "int"},
                              {"name": "contentSize", "type": "long"},
                              {"name": "referrer", "type": "string"},
                              {"name": "useragent", "type": "string"}
                            ],
        "name": "EcommerceLogEvents",
        "type": "record"
    }""""

    schemaString
  }


  def createCountryCodeMap(filename: String): mutable.Map[String, String] = {

    val cntryCodeMap = mutable.Map[String, String]()
    val lines = scala.io.Source.fromFile(filename).getLines()
    while (lines.hasNext) {
      val Array(ipOctets, cntryCode) = lines.next().split(" ")
      cntryCodeMap.put(ipOctets, cntryCode)
    }
    cntryCodeMap
  }


  def bytesToAvroGenriceRecord(bytesRdd:RDD[Array[Byte]], schemaString:String) = {

    bytesRdd.mapPartitions(partitionItr => {
      val parser = new Schema.Parser
      val avroSchema: Schema = parser.parse(schemaString)
      val recordInjection = GenericAvroCodecs.toBinary[GenericRecord](avroSchema);

      partitionItr.map(avroBytes => {
        recordInjection.invert(avroBytes).get
      })
    })

  }


  def getPageViews(genericAvroRdd:RDD[GenericRecord]) = {

    genericAvroRdd.map(avroRecord => avroRecord.get("requestUri").toString)
      .filter(_.length != 0)
      .map(requestURI => {
        logger.debug("requestURI: " + requestURI)
        val productURI = requestURI.split(" ")(1)
        val product = productURI.split("/")(3)
        (product, 1)
      }).reduceByKey(_ + _)

  }

  def getStatusCount(genericAvroRdd:RDD[GenericRecord]) = {
    genericAvroRdd.map(avroRecord => {
      val status = avroRecord.get("status").toString
      logger.debug("status_code: " + status)
      (status, 1)
    }).reduceByKey(_ + _)
  }


  def getReferrer(genericAvroRdd:RDD[GenericRecord]) = {
    genericAvroRdd.map(avroRecord => {
      val referrer = avroRecord.get("referrer").toString
      logger.debug("referrer: " + referrer)
      (referrer, 1)
    }).reduceByKey(_ + _)
  }


  def getVisitsByCounrty(genericAvroRdd:RDD[GenericRecord], countryCodeBroadcast: Broadcast[mutable.Map[String, String]]) = {

    genericAvroRdd.map(avroRecord => avroRecord.get("ipv4").toString)
      .filter(_.length != 0)
      .map(ipaddress => {
        logger.debug("ipaddress: " + ipaddress)
        val countryCodeMap =  countryCodeBroadcast.value
        val octets = ipaddress.toString.split("\\.")
        val key = octets(0) + "." + octets(1)
        val countryCode = countryCodeMap.getOrElse(key, "unknown country")
        (countryCode, 1)
      }).reduceByKey(_ + _)
  }

}
