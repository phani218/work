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

object Pharos_InputArgs {

 
  @Option(name = "-destination_table", required = true,
    usage = "table name in cassandra")
  var destination_table: String = null

  @Option(name = "-destination_keyspace", required = true,
    usage = "keyspace name in cassandra")
  var destination_keyspace: String = null

   @Option(name = "-cassandra_conn_host", required = true,
    usage = "keyspace name in cassandra")
  var cassandra_conn_host: String = null
}


object pharos  {
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
    val sparkConf = new SparkConf().setAppName("pharos")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    //   sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.cassandra.connection.host", Pharos_InputArgs.cassandra_conn_host)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
  


       var startTime = System.currentTimeMillis()
    val start = startTime
    
    val query ="""
      select
      upper(netID) netid,
	'printcredits' as account,
	remaining_balance as remainingbalance
  from
	edw_lnd_dta.pharos_stu_printcredits_lnd
      """
    val printcredits=sqlContext.sql(query)
    log.warn(System.currentTimeMillis().toString() + printcredits.printSchema().toString())
    
    printcredits.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> Pharos_InputArgs.destination_table,
    "keyspace" -> Pharos_InputArgs.destination_keyspace)).save();
  
    var finishTime = System.currentTimeMillis()
    log.warn("Pharos data saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime 
    //}
    //else
    //{
      //log.warn(System.currentTimeMillis() + " Dataframe Empty")
    //}
    
  }

}