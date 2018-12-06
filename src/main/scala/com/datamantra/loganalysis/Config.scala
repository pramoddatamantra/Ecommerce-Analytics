package com.datamantra.loganalysis

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

/**
 * Created by kafka on 15/11/18.
 */
object Config {

  val logger = Logger.getLogger(getClass.getName)

  var setting:Settings = _

  def parseArgs(args: Array[String]) = {

    val applicationConf = ConfigFactory.parseFile(new File(args(0)))

      setting = new Settings(applicationConf)
    }


  def debugSetting() = {

    logger.debug("SparkSettings: " + setting.sparkSettings)
    logger.debug("KafkaSettings: " + setting.kafkaSettings)
    logger.debug("CassandraSetting: " + setting.cassandraSettings)
  }
}
