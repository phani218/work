package com.crg.initialLoad
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Logger
import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, Option }
import scala.collection.JavaConverters._
object eventusdataload {
   
  def main(args: Array[String]) {
   val parser = new CmdLineParser(Pharos_InputArgs)
    try {
      // parser.parseArgument(args)
      parser.parseArgument(args.toList.asJava)

    } catch {
      case e: CmdLineException =>
        print(s"Error:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        System.exit(1)
    }
    
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName("eventus")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    //   sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.cassandra.connection.host", Pharos_InputArgs.cassandra_conn_host)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    var startTime = System.currentTimeMillis()
    val start = startTime
     val query ="""select * from edw_lnd_dta.cal_eventus_lnd"""
       val eventusdata=sqlContext.sql(query)
        eventusdata.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> Pharos_InputArgs.destination_table,
    "keyspace" -> Pharos_InputArgs.destination_keyspace)).save();
  
    var finishTime = System.currentTimeMillis()
    log.warn("Pharos data saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime 
}
}