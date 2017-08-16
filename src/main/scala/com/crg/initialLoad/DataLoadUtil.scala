package com.crg.initialLoad
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
import scala.collection.JavaConverters._
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.spark.sql.functions.udf


object DataLoadUtil {
 /*  @Option(name = "-destination_table", required = true,
    usage = "table name in cassandra")
    var destination_table: String = null
  
    @Option(name = "-destination_keyspace", required = true,
    usage = "keyspace name in cassandra")
    var destination_keyspace: String = null
*/
/*
   * Custom UDF to calculate the hex of sensitive data
   */
  
  def hexDigest: (String => String)={s => DigestUtils.sha256Hex(s).substring(0,10);}

  val hexDigestUDF = udf(hexDigest)

}