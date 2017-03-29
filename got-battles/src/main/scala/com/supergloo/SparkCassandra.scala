package com.supergloo

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scopt.OptionParser


/**
  * Simple Spark Cassandra 
  * One example with Scala case class marshalling
  * Another example using Spark SQL 
  */
object SparkCassandra {

  case class CommandLineArgs (
    cassandra: String = "", // required
    keyspace: String = "gameofthrones", // default is gameofthrones
    limit: Int = 10
  )

  case class Battle(    
    battle_number: Integer,
    year: Integer,
    attacker_king: String,
    defender_king: String
  )

  def main(args: Array[String]) {

    val parser = new scopt.OptionParser[CommandLineArgs]("spark-cassandra-example") {
      head("spark-cassandra-example", "1.0")
      opt[String]('c', "cassandra").required().valueName("<cassandra-host>").
        action((x, c) => c.copy(cassandra = x)).
        text("Setting cassandra is required")
      opt[String]('k', "keyspace").action( (x, c) =>
        c.copy(keyspace = x) ).text("keyspace is a string with a default of `gameofthrones`")
      opt[Int]('l', "limit").action( (x, c) =>
        c.copy(limit = x) ).text("limit is an integer with default of 10")
    }

    parser.parse(args, CommandLineArgs()) match {

      case Some(config) =>
      // do stuff
        val conf = new SparkConf().setAppName("SparkCassandraExampleApp")
        conf.setIfMissing("spark.master", "local[5]")

        conf.set("spark.cassandra.connection.host", config.cassandra)

        val sc = new SparkContext(conf)

        // Spark Cassandra Example one which marshalls to Scala case classes
        val battles:Array[Battle] = sc.cassandraTable[Battle](config.keyspace, "battles").
          select("battle_number","year","attacker_king","defender_king").toArray

        battles.foreach { b: Battle =>
          println("Battle Number %s was defended by %s.".format(b.battle_number, b.defender_king))
        }


        // Spark Cassandra Example Two - Create DataFrame from Spark SQL read
        val sqlContext = new SQLContext(sc)

        val df = sqlContext.read
          .format("org.apache.spark.sql.cassandra")
          .options(Map( "table" -> "battles", "keyspace" -> "gameofthrones" ))
          .load()

        df.show


        // Game of Thrones Battle analysis

        // Who were the most aggressive kings?  (most attacker_kings)
        val countsByAttack = df.groupBy("attacker_king").count().limit(config.limit).sort(desc("count"))
        countsByAttack.show()

        // Which kings were attacked the most?  (most defender_kings)
        val countsByDefend = df.groupBy("defender_king").count().limit(config.limit).sort(desc("count"))
        countsByDefend.show()

        sc.stop()

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }
}