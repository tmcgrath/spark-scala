package com.supergloo
 
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
 
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql.cassandra._


/**
  * Simple Spark Cassandra 
  * One example with Scala case class marshalling
  * Another example using Spark SQL 
  */
object SparkCassandra {

  case class Battle(    
    battle_number: Integer,
    year: Integer,
    attacker_king: String,
    defender_king: String
  )

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SparkCassandraExampleApp")

    if (args.length > 0) conf.setMaster(args(0)) // in case we're running in sbt shell such as: run local[5]

    conf.set("spark.cassandra.connection.host", "127.0.0.1")  // so yes, I'm assuming Cassandra is running locally here.
                                                              // adjust as needed

    val sc = new SparkContext(conf)

    // Spark Cassandra Example one which marshalls to Scala case classes
    val battles:Array[Battle] = sc.cassandraTable[Battle]("gameofthrones", "battles").
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
    val countsByAttack = df.groupBy("attacker_king").count().sort(desc("count"))
    countsByAttack.show()

    // Which kings were attacked the most?  (most defender_kings)
    val countsByDefend = df.groupBy("defender_king").count().sort(desc("count"))
    countsByDefend.show()

    sc.stop()
    
  }
}