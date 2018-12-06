package com.datamantra.loganalysis.spark.model

/**
 * Created by kafka on 3/12/18.
 */
case class EcommerceLogCombined(ipAddress: String, userId: String,
                                   clientId: String, dateTime: String,
                                   requestURI: String, responseCode: Int, contentSize: Long,
                                   referrer:String, useragent:String)

case class EcommerceAccessLog(ipAddress: String, clientId: String,
                           userId: String, dateTime: String, method: String,
                           requestURI: String, protocol: String,
                           responseCode: Int, contentSize: Long)