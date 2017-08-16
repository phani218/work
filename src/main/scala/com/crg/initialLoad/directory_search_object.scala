package com.crg.initialLoad
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.log4j.{ Level, LogManager }
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import scala.collection.Map
import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, Option }
import scala.collection.JavaConverters._

object directory_search_object {

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
    val sparkConf = new SparkConf().setAppName("dirsearch").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    // sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.cassandra.connection.host",Pharos_InputArgs.cassandra_conn_host )
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    var startTime = System.currentTimeMillis()
    val start = startTime

    sqlContext.sql("use edw_lnd_dta")

    val dir_search = """
  Select
pd.last_name fac_last_name,
 pd.first_name fac_first_name,
pd.middle_name fac_middle_name,
 pd.Full_Name full_name,
str_to_map(concat("work_Address_line1=",nvl(ab.Address_line1,''),"~","work_Address_line2=",nvl(ab.Address_line2 ,''),"~",
"work_Address_line3=",nvl(ab.Address_line3,''),"~","work_town_or_city=",nvl(ab.town_or_city,''),"~","work_postal_code=",nvl(ab.postal_code ,''),"~","work_State_code=",nvl(ab.State_code,'')),"~","=") work_address,
concat(nvl(p.NATIONAL_DESTINATION_CODE,''),nvl(p.SUBSCRIBER_NUMBER,'')) phone,
pe.EMAIL_ADDRESS work_email,
pk.netid netid,
pk.banner_pidm banner_pidm,
nvl(ban_spbpers.spbpers_confid_ind,'N') confid_ind,
ph.EMPLOYEE_NUMBER emp_number,
SUBSTR(pha.job_name,3) job_title,
pha.Organization_name organization,
pdsup.FULL_NAME reports_to,
pk_sup.netid sup_netid
From dhub_person_dynamic_fac_lnd  pd
left join dhub_person_static_fac_lnd ps
  ON pd.person_key_value = ps.person_key_value
left join dhub_person_role_fac_lnd pr
  on ps.PERSON_KEY_VALUE = pr.PERSON_ROLE_KEY_VALUE
  and pr.PERSON_ROLE_KEY_VALUE = '2'
left join dhub_person_address_fac_lnd pa
  on pd.person_key_value = pa.person_key_value
Left join dhub_address_type_key_fac_lnd atk
  on pa.address_type_key_value = atk.address_type_key_value
left join dhub_address_fac_lnd aa
  on aa.address_key_value = pa.address_key_value
  and pa.address_type_key_value = '1'
left join dhub_address_fac_lnd ab
  on ab.address_key_value = pa.address_key_value
  and pa.address_type_key_value = '2'
left join dhub_address_fac_lnd ac
  on ac.address_key_value = pa.address_key_value
  and pa.address_type_key_value = '34'
left join dhub_person_phone_fac_lnd pp
  on ps.PERSON_KEY_VALUE = pp.PERSON_KEY_VALUE
left join DHUB_phone_type_key_FAC_LND ptk
  on pp.PHONE_TYPE_KEY_VALUE = ptk.PHONE_TYPE_KEY_VALUE
left join dhub_phone_fac_lnd p
  on pp.PHONE_KEY_VALUE = p.PHONE_KEY_VALUE
  and pp.phone_type_key_value = '3'
left join dhub_person_email_fac_lnd pe
  on ps.PERSON_KEY_VALUE = pe.PERSON_KEY_VALUE
  and pe.EMAIL_TYPE_KEY_VALUE = '397'
left join dhub_person_email_fac_lnd pe2
  on ps.PERSON_KEY_VALUE = pe2.PERSON_KEY_VALUE
  and pe2.EMAIL_TYPE_KEY_VALUE = '1'
left join dhub_person_hr_fac_lnd ph
  on ps.PERSON_KEY_VALUE = ph.PERSON_KEY_VALUE
left join dhub_person_key_fac_lnd pk
  on ps.PERSON_KEY_VALUE = pk.PERSON_KEY_VALUE
left join dhub_person_contact_relationship_fac_lnd pch
  on ps.PERSON_KEY_VALUE = pch.PERSON_KEY_VALUE
left join dhub_person_dynamic_fac_lnd pds
  on pds.PERSON_KEY_VALUE = pch.CONTACT_PERSON_KEY_VALUE
  and pch.CONTACT_TYPE_KEY_VALUE = '2122'
left join dhub_person_dynamic_fac_lnd pdc
  on pdc.PERSON_KEY_VALUE = pch.CONTACT_PERSON_KEY_VALUE
  and pch.CONTACT_TYPE_KEY_VALUE = '2123'
 left join  DHUB_person_hr_assignment_FAC_LND pha
   on pd.PERSON_KEY_VALUE = pha.PERSON_KEY_VALUE
    left join dhub_person_dynamic_fac_lnd pdsup
  on pha.supervisor_key_value = pdsup.PERSON_KEY_VALUE  
  left join dhub_person_key_fac_lnd pk_sup
  on  pdsup.PERSON_KEY_VALUE  = pk_sup.PERSON_KEY_VALUE 
  	left join ban8_stu_spbpers_lnd ban_spbpers on
	pk.banner_pidm = ban_spbpers.spbpers_pidm
 where  pk.netid is not  null
   """
    val data = sqlContext.sql(dir_search);
    data.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> Pharos_InputArgs.destination_table,
      "keyspace" -> Pharos_InputArgs.destination_keyspace)).save();

    var finishTime = System.currentTimeMillis()
    log.warn("faculty_bio_search Saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime

  }

}