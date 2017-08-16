package com.crg.initialLoad

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.log4j.{ Level, LogManager }
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.Logger
import scala.collection.Map
import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, Option }
import scala.collection.JavaConverters._

object studentProfileDataLoad {


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
    
   // val log = LogManager.getRootLogger
    //log.setLevel(Level.INFO)
       var startTime = System.currentTimeMillis()
        val start = startTime
      
     logger.info("Spark Context Initiation Starts")   
     val sparkConf = new SparkConf().setAppName("studentprofile").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.cassandra.connection.host", Pharos_InputArgs.cassandra_conn_host)
     val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    
    var finishTime = System.currentTimeMillis()
    logger.info("Spark Context Initiation Took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime

    val drop_tble_query = """drop table if exists permmanent_address """

    sqlContext.sql(drop_tble_query)

    val query = """
    CREATE TABLE permmanent_address as
    select * from (
    select s.spraddr_pidm, 
    s.spraddr_atyp_code,
    str_to_map(concat("Address_type=",s.spraddr_atyp_code,"~","SPRADDR_STREET_LINE1=",nvl(s.SPRADDR_STREET_LINE1,''),"~","SPRADDR_STREET_LINE2=",nvl(s.SPRADDR_STREET_LINE2,''),"~",
    "SPRADDR_STREET_LINE3=",nvl(s.SPRADDR_STREET_LINE3,''),"~","SPRADDR_CITY=",nvl(s.SPRADDR_CITY,''),"~","SPRADDR_ZIP=",nvl(s.SPRADDR_ZIP,''),"~","SPRADDR_STATE=",nvl(s.SPRADDR_stat_code,'')),"~","=") permanent_address,
    rank() over(partition by s.spraddr_pidm ,s.spraddr_atyp_code order by s.spraddr_seqno desc) rank_rec
    from edw_lnd_dta.ban8_stu_spraddr_lnd s where s.spraddr_atyp_code ="PR") o where o.rank_rec=1
    """
    sqlContext.sql(query)
    //data.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="test", keyspace="dev_datalake").save()

    val drop_tble_query2 = """drop table if exists school_address """
    sqlContext.sql(drop_tble_query2)
    val query_sa = """
    CREATE TABLE school_address as
    select * from (
    select s.spraddr_pidm, 
    s.spraddr_atyp_code,
    str_to_map(concat("Address_type=",s.spraddr_atyp_code,"~","SPRADDR_STREET_LINE1=",nvl(s.SPRADDR_STREET_LINE1,''),"~","SPRADDR_STREET_LINE2=",nvl(s.SPRADDR_STREET_LINE2,''),"~",
    "SPRADDR_STREET_LINE3=",nvl(s.SPRADDR_STREET_LINE3,''),"~","SPRADDR_CITY=",nvl(s.SPRADDR_CITY,''),"~","SPRADDR_ZIP=",nvl(s.SPRADDR_ZIP,''),"~","SPRADDR_STATE=",nvl(s.SPRADDR_stat_code,'')),"~","=") school_address,
    rank() over(partition by s.spraddr_pidm ,s.spraddr_atyp_code order by s.spraddr_seqno desc) rank_rec
    from edw_lnd_dta.ban8_stu_spraddr_lnd s where s.spraddr_atyp_code ="CU") o where o.rank_rec=1
    """
    sqlContext.sql(query_sa)

    val drop_tble_query3 = """drop table if exists address_final """
    sqlContext.sql(drop_tble_query3)
    val query_af = """
    create table address_final as
    select 
    nvl(a.spraddr_pidm,s.spraddr_pidm) pidm,
    a.permanent_address as permanent_address,
    s.school_address as school_address
    from 
    permmanent_address a FULL OUTER JOIN 
    school_address s ON 
    a.spraddr_pidm=s.spraddr_pidm
    """
    sqlContext.sql(query_af)

    val query_student_profile = """
   SELECT iden.spriden_id                                       netid,
       iden.spriden_pidm                                     pidm,
       str_to_map (concat ('pref_first_name=',
                           nvl (persons.spbpers_pref_first_name, ''),
                           '~',
                           'last_name=',
                           nvl (iden.spriden_last_name, ''),
                           '~',
                           'first_name=',
                           nvl (iden.spriden_first_name, ''),
                           '~',
                           'middle_name=',
                           nvl (iden.spriden_mi, '')),
                   '~',
                   '=')
          legal_name,
       address.permanent_address                             home_address,
       address.school_address,
       stu_telephone.primary_phone_no,
       nvl (persons.spbpers_confid_ind, 'N')                 confid_ind,
       cast (to_date (persons.spbpers_birth_date) AS STRING) birth_date,
       str_to_map (concat ('last_name=',
                           nvl (spremrg.spremrg_last_name, ''),
                           '~',
                           'first_name=',
                           nvl (spremrg.spremrg_first_name, '')),
                   '~',
                   '=')
          emergency_contact,
       str_to_map (
          concat ('school_email=', iden.spriden_id, '@creighton.edu'),
          '&',
          '=')
          email,
       str_to_map (concat ('last_name=',
                           nvl (sorfolk.sorfolk_parent_last, ''),
                           '~',
                           'first_name=',
                           nvl (sorfolk.sorfolk_parent_first, ''),
                           '~',
                           'middle_name=',
                           nvl (sorfolk.sorfolk_parent_mi, '')),
                   '~',
                   '=')
          parent,
       str_to_map (concat ('last_name=',
                           nvl (sorfolk_g.sorfolk_parent_last, ''),
                           '~',
                           'first_name=',
                           nvl (sorfolk_g.sorfolk_parent_first, ''),
                           '~',
                           'middle_name=',
                           nvl (sorfolk_g.sorfolk_parent_mi, '')),
                   '~',
                   '=')
          guardian,
       relt.stvrelt_desc                                     emrg_cont_type,
       concat (spremrg.SPREMRG_PHONE_AREA, spremrg.SPREMRG_PHONE_NUMBER)
          emergency_contact_phone
  FROM edw_lnd_dta.ban8_stu_spriden_lnd  iden
       -- LEFT JOIN edw_lnd_dta.ban8_stu_spbpers_lnd persons_pf
       -- on iden.spriden_pidm = persons_pf.spbpers_pidm
       LEFT JOIN default.address_final address
          ON (iden.spriden_pidm = address.pidm)
       LEFT JOIN
       (SELECT sprtele.sprtele_pidm,
               concat (sprtele.sprtele_phone_area,
                       sprtele.sprtele_phone_number)
                  primary_phone_no,
               rank ()
               OVER (PARTITION BY sprtele.sprtele_pidm
                     ORDER BY sprtele.sprtele_seqno DESC)
                  rank_tele
          FROM edw_lnd_dta.ban8_stu_sprtele_lnd sprtele
         WHERE sprtele.sprtele_tele_code = 'MOBL') stu_telephone
          ON (    iden.spriden_pidm = stu_telephone.sprtele_pidm
              AND stu_telephone.rank_tele = 1)
       LEFT JOIN edw_lnd_dta.ban8_stu_spbpers_lnd persons
          ON (iden.spriden_pidm = persons.spbpers_pidm)
       LEFT JOIN edw_lnd_dta.ban8_stu_spremrg_lnd spremrg
          ON (    iden.spriden_pidm = spremrg.spremrg_pidm
              AND spremrg.spremrg_priority = 1)
       LEFT JOIN EDW_LND_DTA.ban8_stu_stvrelt_lnd relt
          ON (spremrg.spremrg_relt_code = relt.stvrelt_code)
       LEFT JOIN edw_lnd_dta.BAN8_STU_sorfolk_LND sorfolk
          ON (    iden.spriden_pidm = sorfolk.sorfolk_pidm
              AND sorfolk.sorfolk_relt_code = 'P')
       LEFT JOIN edw_lnd_dta.BAN8_STU_sorfolk_LND sorfolk_g
          ON (    iden.spriden_pidm = sorfolk_g.sorfolk_pidm
              AND sorfolk_g.sorfolk_relt_code = 'G')
 WHERE iden.spriden_entity_ind != 'C' AND iden.SPRIDEN_CHANGE_IND IS NULL
    """
    val data = sqlContext.sql(query_student_profile)

   // val data_transform = data.map(p => student_record(Option(p.getString(0)), p.getString(1), p.getMap(2), p.getMap(3), p.getMap(4), p.getString(5), p.getString(6), p.getMap(7), p.getMap(8), p.getMap(9), p.getMap(10)))
  //  data_transform.saveToCassandra("dev_datalake", "studentprofile")

    data.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> Pharos_InputArgs.destination_table,
                "keyspace" -> Pharos_InputArgs.destination_keyspace)).save();
    
    //data.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> "studentprofile",
    //"keyspace" -> "dev_datalake")).save();


  }
}
