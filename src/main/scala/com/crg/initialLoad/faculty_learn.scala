package com.crg.initialLoad
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.log4j.{ Level, LogManager }
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, Option }
import scala.collection.JavaConverters._

object faculty_learn {
  
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
    val sparkConf = new SparkConf().setAppName("Hive_Cassandra").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
 // sparkConf.setMaster("yarn-client")
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
   
   
   val faculty_learn="""
   select
  fd.SIRDPCL_PIDM faculty_pidm,
  iden.spriden_id netid,
  str_to_map (concat (     'last_name=',
                           nvl(iden.spriden_last_name,''),
                           '~',
                           'first_name=', 
                           nvl(iden.spriden_first_name,''),
                           '~',
                           'middle_name=',
                           nvl (iden.spriden_mi, '')),
                   '~',
                   '=') faculty_name,
  fd.SIRDPCL_DEPT_CODE dept_code,
  dept.STVDEPT_DESC department,
  fd.SIRDPCL_COLL_CODE school_code,
  coll.STVCOLL_DESC school,
  crse.scbcrse_title as course_title,
  crse.scbcrse_crse_numb course_number,
  ssbsect_subj_code subject_code,
  fa.sirasgn_crn course_crn,
  fa.sirasgn_term_code term_code
  from edw_lnd_dta.ban8_stu_sirdpcl_lnd  fd
  left join edw_lnd_dta.ban8_stu_spriden_lnd  iden
    on fd.SIRDPCL_PIDM = iden.spriden_pidm
    and iden.spriden_change_ind is null
  left join edw_lnd_dta.ban8_stu_stvdept_lnd  dept
    on dept.stvdept_code = fd.SIRDPCL_DEPT_CODE
  left join edw_lnd_dta.ban8_stu_stvcoll_lnd  coll
    on coll.STVCOLL_CODE = fd.SIRDPCL_coll_CODE
   join edw_lnd_dta.ban8_stu_sirasgn_lnd  fa
    on fd.SIRDPCL_PIDM = fa.sirasgn_pidm
    and fa.sirasgn_crn is not null
  left join edw_lnd_dta.ban8_stu_ssbsect_lnd  sec
  on fa.sirasgn_crn = sec.ssbsect_crn
  and sec.ssbsect_term_code=fa.sirasgn_term_code
  left join edw_lnd_dta.ban8_stu_scbcrse_lnd  crse
  on (sec.ssbsect_crse_numb = crse.scbcrse_crse_numb 
  and sec.ssbsect_subj_code = crse.scbcrse_subj_code
  and crse.scbcrse_coll_code= fd.SIRDPCL_coll_CODE 
  ) 
  """
val data = sqlContext.sql(faculty_learn);
   
 val currentterm=currentTerms.select("stvterm_code").collect().map(_.getString(0)).mkString
 
val csemData=data.filter($"term_code"===currentterm)

   
csemData.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> Pharos_InputArgs.destination_table,
"keyspace" -> Pharos_InputArgs.destination_keyspace)).save();

var finishTime = System.currentTimeMillis()
log.warn("faculty_learn Saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
startTime = finishTime 
 
   }
}