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

object assignments {
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
				val sparkConf = new SparkConf().setAppName("studentassignments").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//   sparkConf.setMaster("yarn-client")
		sparkConf.set("spark.cassandra.connection.host", Pharos_InputArgs.cassandra_conn_host)
		val sc = new SparkContext(sparkConf)
		//val sqlContext = new SQLContext(sc)
		val sqlContext = new HiveContext(sc)
		import sqlContext.implicits._
		var startTime = System.currentTimeMillis()
		val start = startTime
		sqlContext.sql("use edw_lnd_dta")

		 val currentTerm1="""
        select 
        stvterm_code,
        case
        when SUBSTRING(stvterm_code,5,2)='10' then  concat(SUBSTRING(stvterm_code,0,4),'40')
        when SUBSTRING(stvterm_code,5,2)='40' then  concat(SUBSTRING(stvterm_code,0,4),'70')
        when SUBSTRING(stvterm_code,5,2)='70' then  concat(cast(SUBSTRING(stvterm_code,0,4) as int)+ 1,'10')
        end as next_sem
        from ban8_stu_stvterm_lnd 
        where  CURRENT_DATE
        between to_date(stvterm_start_date) 
        and to_date(stvterm_end_date)
        and SUBSTRING(stvterm_code,5,2) in ('10','40','70')
        """
   
    val currentTerm2="""
        select
        stvterm_code,
        case
        when SUBSTRING(stvterm_code,5,2)='10' then  concat(SUBSTRING(stvterm_code,0,4),'40')
        when SUBSTRING(stvterm_code,5,2)='40' then  concat(SUBSTRING(stvterm_code,0,4),'70')
        when SUBSTRING(stvterm_code,5,2)='70' then  concat(cast(SUBSTRING(stvterm_code,0,4) as int)+ 1,'10')
        end as next_sem
        from edw_lnd_dta.ban8_stu_stvterm_lnd 
        where 
        CURRENT_DATE < to_date(stvterm_start_date)
        order by stvterm_code ASC
        limit 1
        """ 
     
   var currentTerms=  sqlContext.sql(currentTerm1)
   
    if(currentTerms.head(1).isEmpty)
    {
      currentTerms=sqlContext.sql(currentTerm2)
    }

		
		
		val assign_query = """
		SELECT 
		A.sis_user_id netid,
		A.sis_user_id	 netid_pk,
		--  A.sis_user_id,
		--  A.unique_name,
		--   B.course_id,
		-- B.course_account_id,
		--B.enrollment_id,
		--B.enrollment_term_id,
		C.id assignment_id,
		F.name                                          term_name,
		F.sis_source_id as term_code,
		C.description                                   assign_desc,
		--  regexp_replace (C.description, "</?[^>]*>", "") assign_desc_txt,
		date_format(C.due_at,"yyyy-MM-dd'T'HH:mm:ss'Z'")                                        assign_due,
		C.title                                         assign_title,
		-- C.workflow_state,
		--   C.grading_type,
		C.unlock_at                                     AS assigned_date,
		C.submission_types,
		C.points_possible,
		D.name                                          course_name,
		D.sis_source_id
		FROM edw_lnd_dta.cnv_stu_pseudonym_dim_lnd  A
		JOIN edw_lnd_dta.cnv_stu_enrollment_fact_lnd B
		ON (A.user_id = B.user_id AND A.sis_user_id IS NOT NULL)
		JOIN edw_lnd_dta.cnv_stu_enrollment_term_dim_lnd F
		ON (B.enrollment_term_id = F.id)
		JOIN edw_lnd_dta.cnv_stu_assignment_dim_lnd C
		ON (B.course_id = C.course_id AND C.workflow_state = 'published')
		JOIN edw_lnd_dta.cnv_stu_course_dim_lnd D
		ON (B.course_id = D.id AND D.sis_source_id IS NOT NULL )
		"""
		val data = sqlContext.sql(assign_query)

	    val currentterm=currentTerms.select("stvterm_code").collect().map(_.getString(0)).mkString
    val nxtterm=  currentTerms.select("next_sem").collect().map(_.getString(0)).mkString
   
    val semData=data.filter($"term_code"===currentterm ||$"term_code"===nxtterm)
	
		
		
		semData.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> Pharos_InputArgs.destination_table,
				"keyspace" -> Pharos_InputArgs.destination_keyspace)).save();

		var finishTime = System.currentTimeMillis()
				log.warn("Assignments Saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
				startTime = finishTime

	}
}