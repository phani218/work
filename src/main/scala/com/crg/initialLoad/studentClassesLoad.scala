package com.crg.initialLoad
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.sql.hive.HiveContext
import scala.collection.Map
import com.datastax.spark.connector.cql.CassandraConnector
import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, Option }
import scala.collection.JavaConverters._

object studentClassesLoad {

  
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
    log.setLevel(Level.INFO)
    val sparkConf = new SparkConf().setAppName("studentclassess").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
 // sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.cassandra.connection.host", Pharos_InputArgs.cassandra_conn_host)
    
    CassandraConnector(sparkConf).withSessionDo { session  => session.execute("truncate table prod_datalake.studentclasses") }
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
 
    
    
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._   
   
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

    val classes_query = """
    SELECT
	B.sfrstcr_pidm student_pidm,
	
			H.spriden_id
		 netid,
	C.scbcrse_title course_title,
	E.STVTERM_DESC term_description,
	D.SSBSECT_term_code term_code,
	to_date(term.stvterm_start_date) term_start_dt,
	to_date(term.stvterm_end_date) term_end_dt,
	D.ssbsect_crn term_crn,
	C.SCBCRSE_CRSE_NUMB course_number,
	C.SCBCRSE_SUBJ_CODE subject_code,
	D.ssbsect_seq_numb course_section,
	concat_ws(
		'_',
		D.SSBSECT_term_code,
		C.scbcrse_subj_code,
		C.scbcrse_crse_numb,
		D.ssbsect_seq_numb
	) sis_source_id,
	A.SSRMEET_BEGIN_TIME class_begin_time,
	A.SSRMEET_END_TIME class_end_time,
	to_date(A.ssrmeet_start_date) class_start_dt,
	to_date(A.ssrmeet_end_date) class_end_dt,
	nvl(A.SSRMEET_BLDG_CODE,'TBA') class_building_code,
	nvl(bldg.stvbldg_desc,'To Be Announced') class_held_build_desc,
	A.SSRMEET_ROOM_CODE class_room_code,
	A.SSRMEET_CREDIT_HR_SESS course_credit_hrs,
	concat_ws(
		"-",
		A.SSRMEET_SUN_DAY,
		A.SSRMEET_MON_DAY,
		A.SSRMEET_TUE_DAY,
		A.SSRMEET_WED_DAY,
		A.SSRMEET_THU_DAY,
		A.SSRMEET_FRI_DAY,
		A.SSRMEET_SAT_DAY
	) AS class_schedule,
	NVL(
		
				G.spriden_id
			,
		' '
	) inst_id,
	concat(
			LOWER( 
				G.spriden_id
			 ),
			'@creighton.edu'
		
	) inst_email,
str_to_map(
		concat(
			'last_name=',
			nvl(
				
						G.spriden_last_name
					,
				''
			),
			'~',
			'first_name=',
			nvl(
				
						G.spriden_first_name
				,
				''
			),
			'~',
			'middle_name=',
			nvl(
				G.spriden_mi,
				''
			)
		),
		'~',
		'='
	) instructor_name
FROM
	edw_lnd_dta.ban8_stu_ssrmeet_lnd A
JOIN edw_lnd_dta.ban8_stu_sfrstcr_lnd B ON
	(
		A.ssrmeet_term_code = B.sfrstcr_term_code
		AND A.SSRMEET_CRN = B.SFRSTCR_CRN
		AND a.ssrmeet_mtyp_code <> 'EXAM'
		--DO not display Dropped classes. WE are excluding DC and DD for now
		and B.sfrstcr_rsts_code not in ('DC','DD' )
		--AND A.SSRMEET_SCHD_CODE = 'L'
	)
JOIN edw_lnd_dta.ban8_stu_spriden_lnd H ON
	(
		B.sfrstcr_pidm = H.SPRIDEN_PIDM
		AND H.SPRIDEN_CHANGE_IND IS NULL
	)
JOIN edw_lnd_dta.ban8_stu_ssbsect_lnd D ON
	(
		D.ssbsect_crn = B.SFRSTCR_CRN
		AND D.SSBSECT_term_code = B.sfrstcr_term_code
		AND D.ssbsect_crn = B.SFRSTCR_CRN
	)
JOIN edw_lnd_dta.ban8_stu_stvterm_lnd E ON
	(
		A.ssrmeet_term_code = E.STVTERM_CODE
	)
LEFT JOIN EDW_LND_DTA.ban8_stu_stvbldg_lnd bldg on
	(
		A.SSRMEET_BLDG_CODE = bldg.stvbldg_code
	)
JOIN edw_lnd_dta.ban8_stu_scbcrse_lnd C ON
	(
		C.SCBCRSE_CRSE_NUMB = D.SSBSECT_CRSE_NUMB
		AND C.SCBCRSE_SUBJ_CODE = D.SSBSECT_SUBJ_CODE
	)
JOIN(
		SELECT
			MAX( b_2.scbcrse_eff_term ),
			scbcrse_subj_code,
			scbcrse_crse_numb,
			scbcrse_eff_term
		FROM
			edw_lnd_dta.ban8_stu_scbcrse_lnd b_2
		GROUP BY
			scbcrse_subj_code,
			scbcrse_crse_numb,
			scbcrse_eff_term
	) max_eff_term ON
	(
		max_eff_term.scbcrse_subj_code = D.ssbsect_subj_code
		AND max_eff_term.scbcrse_crse_numb = D.ssbsect_crse_numb --AND max_eff_term.scbcrse_eff_term = D.ssbsect_term_code

	)
LEFT JOIN edw_lnd_dta.ban8_stu_sirasgn_lnd F ON
	(
		F.sirasgn_term_code = A.ssrmeet_term_code
		AND F.sirasgn_crn = A.ssrmeet_crn
		AND F.sirasgn_category = A.ssrmeet_catagory
		AND F.sirasgn_primary_ind = 'Y'
	)
LEFT JOIN edw_lnd_dta.ban8_stu_spriden_lnd G ON
	(
		G.spriden_pidm = F.sirasgn_pidm
		AND G.spriden_change_ind IS NULL
	)
	LEFT JOIN 	edw_lnd_dta.ban8_stu_stvterm_lnd term ON 
	(
	term.stvterm_code = D.SSBSECT_term_code
	)
WHERE
	max_eff_term.scbcrse_eff_term <= D.ssbsect_term_code
	AND C.SCBCRSE_EFF_TERM = max_eff_term.scbcrse_eff_term
  """
    
    val data = sqlContext.sql(classes_query)
    
    val currentterm=currentTerms.select("stvterm_code").collect().map(_.getString(0)).mkString
    val nxtterm=  currentTerms.select("next_sem").collect().map(_.getString(0)).mkString
   
    val semData=data.filter($"term_code"===currentterm ||$"term_code"===nxtterm)
    
    semData.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> Pharos_InputArgs.destination_table,
    "keyspace" -> Pharos_InputArgs.destination_keyspace)).save();
    
   log.info("Insert into Student Classes table cassandra finsihed")

    
   }
  
}