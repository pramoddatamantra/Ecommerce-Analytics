package com.datamantra.loganalysis.cassandra

import com.datamantra.loganalysis.Config
import org.apache.log4j.Logger

/**
 * Created by kafka on 15/11/18.
 */
object CassandraConfig {

  val logger = Logger.getLogger(getClass.getName)

  var cassandrHost: String = _
  var keyspace: String = _
  var pageViewsTable: String = _
  var statusCounterTable: String = _
  var visitsByCountryTable: String = _
  var referrerCounterTable: String = _

  /*Configuration setting are loaded from application.conf when you run Spark Standalone cluster*/
  def load() = {
    logger.info("Loading Cassandra Setttings")
    cassandrHost = Config.applicationConf.getString("cassandra.hostname")
    keyspace = Config.applicationConf.getString("cassandra.keyspace")
    pageViewsTable = Config.applicationConf.getString("cassandra.table.page.views")
    statusCounterTable = Config.applicationConf.getString("cassandra.table.status.counter")
    visitsByCountryTable = Config.applicationConf.getString("cassandra.table.visit.by.country")
    referrerCounterTable = Config.applicationConf.getString("cassandra.table.referrer.counter")

  }

  /* Default Settings will be used when you run the project from Intellij */
  def defaultSettng() = {
    cassandrHost = "localhost"
    keyspace = "creditcard"
    pageViewsTable = "page_views"
    statusCounterTable = "status_counter"
    visitsByCountryTable = "visits_by_country"
    referrerCounterTable = "customer"
  }

}
