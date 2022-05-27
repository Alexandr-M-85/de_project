package ru.dmp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType

import java.sql.DriverManager
import java.util.Properties

object AppEl{
    def main(args: Array[String]) {
      val spark = SparkSession
        .builder()
        .master("yarn")
        .enableHiveSupport()
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()

      val url = "jdbc:postgresql://ingress-1.prod.dmp.xxx.ru:5448/demo"
      val props = new Properties()
      props.setProperty("user", args(0))
      props.setProperty("password", args(1))
      props.setProperty("driver", "org.postgresql.Driver")
      val con = DriverManager.getConnection(url, props)


      val pgSeats = "bookings.seats"
      val pgSeatsDF = spark.read.jdbc(url, pgSeats, props)

      pgSeatsDF
        .write
        .format("hive")
        .mode("overwrite")
        .saveAsTable("school_de.seats_amomotov")

      val pgFlightsV = "bookings.flights_v"
      val pgFlightsVDF = spark.read.jdbc(url, pgFlightsV, props)

      pgFlightsVDF
        .withColumn("data_actual_departure", col("actual_departure").cast(DateType))
        .write
        .format("hive")
        .mode("overwrite")
        .partitionBy("data_actual_departure")
        .saveAsTable("school_de.flights_v_amomotov")



  }
}
