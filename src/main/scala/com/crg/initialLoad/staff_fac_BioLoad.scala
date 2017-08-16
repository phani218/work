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

object staffbio_InputArgs {

  @Option(name = "-destination_keyspace", required = true,
    usage = "keyspace name in cassandra")
  var destination_keyspace: String = null

     @Option(name = "-cassandra_conn_host", required = true,
    usage = "keyspace name in cassandra")
  var cassandra_conn_host: String = null
  
}


object staff_fac_BioLoad {
  
  
  def main(args: Array[String]) {
  
    
       val parser = new CmdLineParser(staffbio_InputArgs)
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
    val sparkConf = new SparkConf().setAppName("staff-faculty").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
 // sparkConf.setMaster("yarn-client")
    sparkConf.set("spark.cassandra.connection.host", staffbio_InputArgs.cassandra_conn_host)
    val sc = new SparkContext(sparkConf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._   
    var startTime = System.currentTimeMillis()
        val start = startTime
    
 sqlContext.sql("use edw_lnd_dta")
 
    val staff_bio ="""
 Select
str_to_map(concat('pref_first_name=',nvl( pd.known_as,''),'~','last_name=',nvl( pd.last_name,''),'~',
'first_name=',nvl( pd.first_name,''),'~','middle_name=',nvl(pd.middle_name,'')),'~','=') staff_name,
 pd.Full_Name full_name,
str_to_map(concat("home_Address_line1=",nvl(aa.Address_line1,''),"~","home_Address_line2=",nvl(aa.Address_line2 ,''),"~",
"home_Address_line3=",nvl(aa.Address_line3,''),"~","home_town_or_city=",nvl(aa.town_or_city,''),"~","home_postal_code=",nvl(aa.postal_code ,''),"~","home_State_code=",nvl(aa.State_code,'')),"~","=") faculty_address,

str_to_map(concat("work_Address_line1=",nvl(ab.Address_line1,''),"~","work_Address_line2=",nvl(ab.Address_line2 ,''),"~",
"work_Address_line3=",nvl(ab.Address_line3,''),"~","work_town_or_city=",nvl(ab.town_or_city,''),"~","work_postal_code=",nvl(ab.postal_code ,''),"~","work_State_code=",nvl(ab.State_code,'')),"~","=") work_address,
str_to_map(concat("mail_Address_line1=",nvl(ac.Address_line1,''),"~","mail_Address_line2=",nvl(ac.Address_line2 ,''),"~",
"mail_Address_line3=",nvl(ac.Address_line3,''),"~","mail_town_or_city=",nvl(ac.town_or_city,''),"~","mail_postal_code=",nvl(ac.postal_code ,''),"~","mail_State_code=",nvl(ac.State_code,'')),"~","=") mail_address,
concat(nvl(p.NATIONAL_DESTINATION_CODE,''),nvl(p.SUBSCRIBER_NUMBER,'')) phone,
pe.EMAIL_ADDRESS work_email,
pe2.EMAIL_ADDRESS personal_email,
ps.date_of_birth,
pk.netid netid,
pk.banner_pidm,
ph.EMPLOYEE_NUMBER emp_number,
pd.MARITAL_STATUS marital_status,
str_to_map(concat('last_name=',nvl( spremrg.spremrg_last_name,''),'~',
'first_name=',nvl( spremrg.spremrg_first_name,''),'~','middle_name=',nvl(pd.middle_name,'')),'~','=') emergency_contact,
relt.stvrelt_desc emrg_cont_type,
concat(spremrg.SPREMRG_PHONE_AREA,spremrg.SPREMRG_PHONE_NUMBER) emergency_contact_phone
From dhub_person_dynamic_fac_lnd  pd
left join dhub_person_static_fac_lnd ps
  ON pd.person_key_value = ps.person_key_value
left join dhub_person_role_fac_lnd pr
  on ps.PERSON_KEY_VALUE = pr.PERSON_ROLE_KEY_VALUE
  and pr.PERSON_ROLE_KEY_VALUE = '2'
left join dhub_person_address_fac_lnd pa
  on pd.person_key_value = pa.person_key_value
            AND pa.address_type_key_value = '1'
Left join dhub_address_type_key_fac_lnd atk
  on pa.address_type_key_value = atk.address_type_key_value
LEFT JOIN dhub_person_address_fac_lnd pa2
         ON pd.person_key_value = pa2.person_key_value
       and  pa2.address_type_key_value = '2'
       LEFT JOIN dhub_address_type_key_fac_lnd atk2
          ON pa2.address_type_key_value = atk2.address_type_key_value  
left join dhub_address_fac_lnd aa
  on aa.address_key_value = pa.address_key_value
  and pa.address_type_key_value = '1'
left join dhub_address_fac_lnd ab
  on ab.address_key_value = pa2.address_key_value
  and pa2.address_type_key_value = '2'
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
  left join ban8_stu_spremrg_lnd spremrg
  on (spremrg.spremrg_pidm = pk.banner_pidm and spremrg.spremrg_priority=1)
  left join EDW_LND_DTA.ban8_stu_stvrelt_lnd relt
  on(spremrg.spremrg_relt_code=relt.stvrelt_code)
where  pk.netid is not  null
"""

val data = sqlContext.sql(staff_bio);
    
     data.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> "staff_bio",
    "keyspace" -> staffbio_InputArgs.destination_keyspace)).save();
  
    var finishTime = System.currentTimeMillis()
     log.warn("Staff Bio Saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime
   
   
     val facuty_query ="""
   Select
str_to_map(concat('pref_first_name=',nvl( pd.known_as,''),'~','last_name=',nvl( pd.last_name,''),'~',
'first_name=',nvl( pd.first_name,''),'~','middle_name=',nvl(pd.middle_name,'')),'~','=') faculty_name,
pd.Full_Name full_name,
str_to_map(concat("home_Address_line1=",nvl(aa.Address_line1,''),"~","home_Address_line2=",nvl(aa.Address_line2 ,''),"~",
"home_Address_line3=",nvl(aa.Address_line3,''),"~","home_town_or_city=",nvl(aa.town_or_city,''),"~","home_postal_code=",nvl(aa.postal_code ,''),"~","home_State_code=",nvl(aa.State_code,'')),"~","=") faculty_address,

str_to_map(concat("work_Address_line1=",nvl(ab.Address_line1,''),"~","work_Address_line2=",nvl(ab.Address_line2 ,''),"~",
"work_Address_line3=",nvl(ab.Address_line3,''),"~","work_town_or_city=",nvl(ab.town_or_city,''),"~","work_postal_code=",nvl(ab.postal_code ,''),"~","work_State_code=",nvl(ab.State_code,'')),"~","=") work_address,
str_to_map(concat("mail_Address_line1=",nvl(ac.Address_line1,''),"~","mail_Address_line2=",nvl(ac.Address_line2 ,''),"~",
"mail_Address_line3=",nvl(ac.Address_line3,''),"~","mail_town_or_city=",nvl(ac.town_or_city,''),"~","mail_postal_code=",nvl(ac.postal_code ,''),"~","mail_State_code=",nvl(ac.State_code,'')),"~","=") mail_address,
concat(nvl(p.NATIONAL_DESTINATION_CODE,''),nvl(p.SUBSCRIBER_NUMBER,'')) phone,
pe.EMAIL_ADDRESS work_email,
pe2.EMAIL_ADDRESS personal_email,
ps.date_of_birth,
pk.netid netid,
pk.banner_pidm,
ph.EMPLOYEE_NUMBER emp_number,
pd.MARITAL_STATUS marital_status,
str_to_map(concat('last_name=',nvl( spremrg.spremrg_last_name,''),'~',
'first_name=',nvl( spremrg.spremrg_first_name,''),'~','middle_name=',nvl(pd.middle_name,'')),'~','=') emergency_contact,
pds.FULL_NAME spouse_name,
pdc.FULL_NAME child_name,
relt.stvrelt_desc emrg_cont_type,
concat(spremrg.SPREMRG_PHONE_AREA,spremrg.SPREMRG_PHONE_NUMBER) emergency_contact_phone
From dhub_person_dynamic_fac_lnd pd
left join dhub_person_static_fac_lnd ps
  ON pd.person_key_value = ps.person_key_value
left join dhub_person_role_fac_lnd pr
  on ps.PERSON_KEY_VALUE = pr.PERSON_ROLE_KEY_VALUE
  and pr.PERSON_ROLE_KEY_VALUE = '5'
left join dhub_person_address_fac_lnd pa
  on pd.person_key_value = pa.person_key_value
              AND pa.address_type_key_value = '1'

Left join dhub_address_type_key_fac_lnd atk
  on pa.address_type_key_value = atk.address_type_key_value
  LEFT JOIN dhub_person_address_fac_lnd pa2
         ON pd.person_key_value = pa2.person_key_value
       and  pa2.address_type_key_value = '2'
       LEFT JOIN dhub_address_type_key_fac_lnd atk2
          ON pa2.address_type_key_value = atk2.address_type_key_value  

left join dhub_address_fac_lnd aa
  on aa.address_key_value = pa.address_key_value
  and pa.address_type_key_value = '1'
left join dhub_address_fac_lnd ab
  on ab.address_key_value = pa2.address_key_value
  and pa2.address_type_key_value = '2'
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
  left join ban8_stu_spremrg_lnd spremrg
  on (spremrg.spremrg_pidm = pk.banner_pidm and spremrg.spremrg_priority=1)
  left join EDW_LND_DTA.ban8_stu_stvrelt_lnd relt
  on(spremrg.spremrg_relt_code=relt.stvrelt_code)
  where  pk.netid is not  null
    """  
 val fac_data=  sqlContext.sql(facuty_query); 

     fac_data.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> "faculty_bio",
    "keyspace" -> staffbio_InputArgs.destination_keyspace)).save();
      finishTime = System.currentTimeMillis()
     log.warn("Faculty Bio Saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime
   
    
    val staff_prof="""
      Select
  pk.netid  netid,
 ps.person_key_value person_key_value,
pha.job_name job_title,
pha.Organization_name organization,
--Department
pd.FULL_NAME reports_to,
 ph.Original_date_of_hire  hire_date,
--pha.payroll_frequency payroll,
nvl(pha.Assignment_Number,'NA') assignment_number
--Expertise/Skills
--Applications/Tools
--Group/Additional MetaData
from dhub_person_dynamic_fac_lnd  ps
left join dhub_person_role_fac_lnd  pr
  on ps.PERSON_KEY_VALUE = pr.PERSON_KEY_VALUE
  and pr.PERSON_ROLE_KEY_VALUE = '2'
   left join dhub_person_key_fac_lnd  pk
    on ps.PERSON_KEY_VALUE = pk.PERSON_KEY_VALUE
left join EDW_LND_DTA.DHUB_person_hr_assignment_FAC_LND  pha
  on ps.PERSON_KEY_VALUE = pha.PERSON_KEY_VALUE
 -- and pha.primary_flag='Y'
left join dhub_person_hr_fac_lnd  ph
  on ps.PERSON_KEY_VALUE = ph.PERSON_KEY_VALUE
left join dhub_person_dynamic_fac_lnd pd
  on pha.supervisor_key_value = pd.PERSON_KEY_VALUE
  where pk.netid is not null
      """
 
    val sp_data=  sqlContext.sql(staff_prof); 

   sp_data.write.format("org.apache.spark.sql.cassandra").mode("append").options(Map("table" -> "staff_professional",
    "keyspace" -> staffbio_InputArgs.destination_keyspace)).save();   
 
     finishTime = System.currentTimeMillis()
     log.warn("Staff Professional Saved to Cassandra, took " + (finishTime - startTime).toString + " ms.")
    startTime = finishTime
   
  }
  
}