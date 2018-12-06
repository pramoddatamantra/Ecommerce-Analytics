package com.datamantra.loganalysis.cassandra

/**
 * Created by kafka on 15/11/18.
 */
case class CassandraSettings(
  hostname:String,
  keyspace:String,
  pageViewsTable:String,
  statusCounterTable:String,
  visitsByCountryTable:String,
  referrerCounterTable:String,
  outputBatchSize:String
)
