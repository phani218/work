package com.crg.initialLoad
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
trait inputArgs {
   @Option(name = "-destination_table", required = true,
    usage = "table name in cassandra")
    var destination_table: String = null
  
    @Option(name = "-destination_keyspace", required = true,
    usage = "keyspace name in cassandra")
    var destination_keyspace: String = null
}