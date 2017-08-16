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
import org.apache.spark.sql.functions.{ rowNumber, max, broadcast }
import org.apache.spark.sql.expressions.Window
import com.databricks.spark.csv

object InputArgs {

  @Option(name = "-inputdir", required = true,
    usage = "hdfs dir for input dir ex: /hdfslnd/<>")
  var inputdir: String = null

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

object atriumFileLoad {

  /*
   * Main Method Starts Here
   */

  def main(args: Array[String]) {

    val parser = new CmdLineParser(InputArgs)
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
    val sparkConf = new SparkConf().setAppName("AtriumFileLoad")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    //   sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.cassandra.connection.host", InputArgs.cassandra_conn_host)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    var startTime = System.currentTimeMillis()
    val start = startTime

    val df = sqlContext.
      read.
      format("com.databricks.spark.csv").
      option("header", "false").
      option("inferSchema", "true").
      load(InputArgs.inputdir)

    val filData = df.filter("C1 != ''")
    val w = Window.partitionBy($"C0").orderBy($"C4".desc)

    val latestJayBucks = filData.withColumn("rn", rowNumber.over(w)).where($"rn" === 1).drop("rn")

    val newNames = Seq("campusid", "netid", "account", "remainingbalance", "processtimestamp", "reader", "grossamt")
   
    val JayBucks = latestJayBucks  
      .toDF(newNames: _*)
      .drop("processtimestamp")
      .drop("reader")
      .drop("grossamt");
      
    val JayBucksBal=JayBucks.drop("campusid")

    JayBucksBal.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> InputArgs.destination_table,
      "keyspace" -> InputArgs.destination_keyspace)).save();

    var finishTime = System.currentTimeMillis()
    log.warn("JayBucks data saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")

  }

}