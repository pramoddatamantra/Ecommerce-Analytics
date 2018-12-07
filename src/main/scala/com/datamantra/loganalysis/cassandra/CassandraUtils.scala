package com.datamantra.loganalysis.cassandra

import java.beans.Statement

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

/**
 * Created by kafka on 7/12/18.
 */
object CassandraUtils {

  val logger = Logger.getLogger(getClass.getName)

  def updateToCassandra(rdd:RDD[(String, Int)], connector:CassandraConnector, keyspace:String, table:String, keyColumn:String) = {
    rdd.foreach{
      case (key:String, value:Int) => {
      logger.debug("key: " + key + " count: " + value)
      connector.withSessionDo(session => {
        session.execute(s"update ${keyspace}.${table} set count = count + ${value} where ${keyColumn} = '${key}'")
      })
    }}
  }

}
