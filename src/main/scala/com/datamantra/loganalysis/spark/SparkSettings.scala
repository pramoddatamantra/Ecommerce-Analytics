package com.datamantra.loganalysis.spark

/**
 * Created by kafka on 15/11/18.
 */
case class SparkSettings(
  master:String,
  name:String,
  batchInterval:String,
  windowInterval:Int,
  maxRatePerPartition:String,
  ipLookupFile:String
)

object  SparkSettings {

  def loadSparkConfig() = {

  }
}
