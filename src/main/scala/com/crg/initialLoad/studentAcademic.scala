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

object studentAcademic {
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
    
    
    val loggerName = this.getClass.getName
    lazy val logger = Logger.getLogger(loggerName)
    logger.info("Spark Context Initiation Starts")    
    val sparkConf = new SparkConf().setAppName("studentacademic").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
 // sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.cassandra.connection.host", Pharos_InputArgs.cassandra_conn_host)
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

      var startTime = System.currentTimeMillis()
        val start = startTime
    
        sqlContext.sql("use edw_lnd_dta");
        
    val studentacad_query = """
           SELECT student_pidm,
       netid,
       term_code,
       major_code,
       major_desc,
       advisor_id,
       advisor,
       advisoremail,
       cumulative_gpa,
       level_code,
       class_code,
       class,
       college_code,
       college_description,
       minor
  FROM (SELECT cuvenr3_pidm                      student_pidm,
               
                           E.spriden_id
                  netid,
               C.stvmajr_desc                    minor,
               cuv.cuvenr3_coll_code_1           college_code,
               coll.stvcoll_desc                 college_description,
               cuv.cuvenr3_class                 class_code,
               cuv.cuvenr3_term_code             term_code,
               cuv.cuvenr3_majr_code_1           major_code,
               B.stvmajr_desc                    major_desc,
               gpa.shrlgpa_gpa                   cumulative_gpa,
               nvl (gpa.shrlgpa_levl_code, '00') level_code,
               CASE cuv.cuvenr3_class
                  WHEN '01' THEN 'First Year'
                  WHEN '02' THEN 'Second Year'
                  WHEN '03' THEN 'Third Year'
                  WHEN '04' THEN 'Fourth Year'
                  WHEN 'GR' THEN 'Graduate'
                  WHEN 'FR' THEN 'Freshman'
                  WHEN 'SO' THEN 'Sophomore'
                  WHEN 'JR' THEN 'Junior'
                  WHEN 'SR' THEN 'Senior'
                  WHEN 'SP' THEN 'Special'
                  WHEN 'UN' THEN 'Unclassified'
                  ELSE ''
               END
                  AS class,
               str_to_map (concat ('last_name=',
                                   nvl (cuv.cuvenr3_adv_lname, ''),
                                   '~',
                                   'first_name=',
                                   nvl (cuv.cuvenr3_adv_fname, '')),
                           '~',
                           '=')
                  advisor,
               cuv.cuvenr3_adv_email             advisoremail,
               cuv.cuvenr3_adv_id                advisor_id,
               rank ()
               OVER (PARTITION BY cuv.cuvenr3_pidm
                     ORDER BY cuv.cuvenr3_term_code DESC)
                  rank
          FROM ban8_stu_cuvenr3_lnd  cuv
               INNER JOIN ban8_stu_spriden_lnd E
                  ON     E.spriden_pidm = cuv.cuvenr3_pidm
                     AND E.spriden_change_ind IS NULL
               INNER JOIN ban8_stu_stvmajr_lnd B
                  ON b.stvmajr_code = cuv.cuvenr3_majr_code_1
               LEFT OUTER JOIN ban8_stu_stvmajr_lnd C
                  ON C.stvmajr_code = cuv.cuvenr3_majr_code_minr_1
               LEFT JOIN ban8_stu_stvcoll_lnd coll
                  ON (coll.stvcoll_code = cuv.cuvenr3_coll_code_1)
               LEFT JOIN ban8_stu_shrlgpa_lnd gpa
                  ON (    gpa.shrlgpa_pidm = E.spriden_pidm
                      AND gpa.shrlgpa_gpa_type_ind = 'O')
       ) A
 WHERE A.rank = 1
                            """
    val data = sqlContext.sql(studentacad_query)
   
     var finishTime = System.currentTimeMillis()
    logger.info("Hive content read, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime
    
    data.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> Pharos_InputArgs.destination_table,
    "keyspace" -> Pharos_InputArgs.destination_keyspace)).save();
    
    finishTime = System.currentTimeMillis()
    logger.info("Student Academinc Data Saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime
  
  }
}