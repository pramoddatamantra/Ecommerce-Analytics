package com.datamantra.loganalysis.spark

import com.datamantra.loganalysis.Settings
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DecoderFactory, Decoder, DatumReader}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.log4j.Logger

import scala.collection.mutable

//import com.twitter.bijection.avro.GenericAvroCodecs
import io.confluent.kafka.schemaregistry.client.rest.RestService
import org.apache.avro.Schema
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{MultiThreadedHttpConnectionManager, HttpClient}

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

}
