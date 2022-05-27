package ru.dmp

import io.netty.handler.codec.dns.DnsQuestion
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, avg, col, collect_list, concat_ws, count, countDistinct, date_format, dayofmonth, desc, floor, lit, month, rank, sum, to_date, to_timestamp, to_utc_timestamp, unix_timestamp, year}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

import java.sql.DriverManager
import java.util.Properties

object AppCalc{
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("yarn")
      .config("spark.driver.extraJavaOptions","-Duser.timezone=GMT")
      .config("spark.executor.extraJavaOptions","-Duser.timezone=GMT")
      .config("spark.sql.session.timeZone","UTC")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val tickets = spark.table("school_de.bookings_tickets")
    val airports = spark.table("school_de.bookings_airports")
    val bookings = spark.table("school_de.bookings_bookings")
    val ticket_flights = spark.table("school_de.bookings_ticket_flights")
    val flights_v = spark.table("school_de.bookings_flights_v")
    val routes = spark.table("school_de.bookings_routes")
    val aircrafts = spark.table("school_de.bookings_aircrafts")
    val flights = spark.table("school_de.bookings_flights")


    val RESULT_TABLE = "school_de.results_amomotov"


//        Question1

    val res1 = tickets
      .groupBy("book_ref")
      .agg(count("passenger_id").as("res"))
      .select(
        lit(1).as("id"),
        col("res").cast("string").as("response")
      )
      .orderBy(desc("response"))
      .limit(1)




//        Question2


    val avgPass = tickets
      .groupBy("book_ref")
      .agg(count("passenger_id").as("res"))
      .select(avg("res"))
      .collect()(0)(0)

    val res23 = tickets
      .groupBy("book_ref")
      .agg(count("passenger_id").as("res"))
      .where(col("res") > avgPass)
      .count()

    val columns2 = Seq("id", "response")
    val data2 = Seq((2,res23))
    val res2 = spark.createDataFrame(data2).toDF(columns2: _*)



//    Question3

    val max_pass = tickets
      .groupBy("book_ref")
      .agg(count("passenger_id").as("count"))
      .orderBy(desc("count"))
      .select("count")
      .collect()(0)(0)

    val res3 = tickets
      .groupBy("book_ref")
      .agg(count("passenger_id").as("count"))
      .filter(col("count")===max_pass)
      .withColumnRenamed("book_ref", "book_ref1")
      .join(tickets,col("book_ref1")===tickets("book_ref"))
      .groupBy("book_ref")
      .agg(concat_ws(" ", collect_list("passenger_id")).as("pass_list"))
      .groupBy("pass_list").count()
      .where(col("count")>1)
      .groupBy().count()
      .select(
        lit(3).as("id"),
        col("count").as("response")
      )


//       Question4

    val res4 = tickets
      .groupBy("book_ref")
      .agg(count("passenger_id").as("pass_count"))
      .filter(col("pass_count") === 3)
      .join(tickets,"book_ref")
      .select(
        lit(4).as("id"),
        concat_ws("|",
        col("book_ref"),
        col("passenger_id"),
        col("passenger_name"),
        col("contact_data")).as("response")
      )
      .orderBy("response")


//    Question5

    val res5 = tickets
      .join(bookings,"book_ref")
      .join(ticket_flights,"ticket_no")
      .groupBy("book_ref")
      .agg(count("flight_id").as("count"))
      .select(
        lit(5).as("id"),
        functions.max("count").as("response")
      )



//    Question6

    val res6 = tickets
      .join(bookings,"book_ref")
      .join(ticket_flights,"ticket_no")
      .groupBy("book_ref","passenger_id")
      .agg(count("flight_id").as("count"))
      .select(
        lit(6).as("id"),
        functions.max("count").as("response")
      )



//    Question7

    val res7 = flights
      .join(ticket_flights,"flight_id")
      .join(tickets, "ticket_no")
      .groupBy("passenger_id")
      .count().as("count")
      .select(
        lit(7).as("id"),
        functions.max("count").as("response")
      )


//    Question8

    val res8 = ticket_flights
      .join(tickets,"ticket_no")
      .join(flights,"flight_id")
      .filter(col("status")=!=("Cancelled"))
      .groupBy("passenger_id","passenger_name","contact_data")
      .agg(sum("amount").as("total_amount"))
      .orderBy("total_amount","passenger_id","passenger_name","contact_data")
      .withColumn("num", rank().over(Window.orderBy(col("total_amount"))))
      .filter(col("num")===1)
      .select(
        lit(8).as("id"),
        concat_ws("|",
          col("passenger_id"),
          col("passenger_name"),
          col("contact_data"),
          col("total_amount")).as("response")
            )
      .orderBy("response")



//    Question9

    val res9 = ticket_flights
      .join(tickets, "ticket_no")
      .join(flights_v, "flight_id")
      .filter(col("status")===("Arrived"))
      .withColumn("act_duration", to_timestamp(col("actual_duration"), "HH:mm:ss"))
      .groupBy("passenger_id","passenger_name","contact_data")
      .agg(sum("act_duration").as("total_time"))
      .withColumn("num", rank().over(Window.orderBy(col("total_time").desc)))
      .filter(col("num")===1)
      .withColumn("hour", floor(col("total_time")/3600))
      .withColumn("minute", floor((col("total_time")%3600)/60))
      .withColumn("sec", lit("00"))
      .withColumn("time", concat_ws(":",col("hour"),col("minute"),col("sec")))
      .drop("hour","minute","sec")
      .select(
        lit(9).as("id"),
        concat_ws("|",
          col("passenger_id"),
          col("passenger_name"),
          col("contact_data"),
          col("time")).as("response")
      )
      .orderBy("response")



//    Question10


    val res10 = airports
      .groupBy("city")
      .agg(count("city").as("res"))
      .where(col("res")>1)
      .orderBy("city")
      .select(
        lit(10).as("id"),
        col("city").as("response")
      )
      .orderBy("response")



//    Question11

    val res11 = routes
      .groupBy("departure_city")
      .agg(countDistinct("arrival_city").as("count_city"))
      .withColumn("num", rank().over(Window.orderBy(col("count_city"))))
      .filter(col("num")===1)
      .select(
        lit(11).as("id"),
        col("departure_city").as("response")
      )
      .orderBy("response")



//    Question12

    val res12 = airports
      .withColumn("departure_city", col("city"))
      .select("departure_city")
      .crossJoin(airports.withColumn("arrival_city",col("city"))
      .select("arrival_city"))
      .filter(col("departure_city") =!= col("arrival_city"))
      .except(
        flights_v.select("departure_city", "arrival_city").distinct()
      )
      .filter(col("departure_city")<col("arrival_city"))
      .orderBy("departure_city", "arrival_city")
      .select(
        lit(12).as("id"),
        concat_ws("|",
          col("departure_city"),
          col("arrival_city")).as("response")
      )
      .orderBy("response")



//    Question13


    val res13 = routes
      .filter(col("arrival_city")=!=("Москва"))
      .select(col("arrival_city")).distinct()
      .except(
        routes
          .filter(col("departure_city")===("Москва"))
          .select(col("arrival_city")).distinct()
      )
      .select(
        lit(13).as("id"),
        col("arrival_city").as("response")
      )
      .orderBy("response")



//    Question14

    val aircraft_code = flights_v
      .filter(col("status")===("Arrived"))
      .groupBy(col("aircraft_code"))
      .agg(count("flight_id").as("count_f"))
      .withColumn("num",rank().over(Window.orderBy(col("count_f").desc)))
      .filter(col("num")===1)
      .select(col("aircraft_code"))
      .collect()(0)(0)

    val res14 = aircrafts
      .filter(col("aircraft_code")===aircraft_code)
      .select(
        lit(14).as("id"),
        col("model").as("response")
      )



//    Question15

    val aircraft_code2 = flights
      .join(ticket_flights,"flight_id")
      .filter(col("status")===("Arrived"))
      .groupBy("aircraft_code")
      .agg(count("ticket_no").as("res"))
      .withColumn("num",rank().over(Window.orderBy(col("res").desc)))
      .filter(col("num")===1)
      .select(col("aircraft_code"))
      .collect()(0)(0)

    val res15 = aircrafts
      .filter(col("aircraft_code")===aircraft_code2)
      .select(
        lit(15).as("id"),
        col("model").as("response")
      )
      .orderBy("response")


//    Question16

    val res16 = flights_v
      .filter(col("status")===("Arrived"))
      .withColumn("act_duration", unix_timestamp(col("actual_duration"), "HH:mm:ss"))
      .withColumn("sched_duration", unix_timestamp(col("scheduled_duration"), "HH:mm:ss"))
      .select(
        lit(16).as("id"),
        (abs(sum(col("act_duration")) - sum(col("sched_duration"))) / 60).
          cast("int").as("response")
      )




//    Question17

    val res17 = flights_v
      .filter(col("status").isin("Arrived", "Departed"))
      .filter(col("departure_city")===("Санкт-Петербург"))
      .withColumn("actual_date", to_utc_timestamp(col("actual_departure"), "Europe/Moscow"))
      .withColumn("year", year(col("actual_date")))
      .withColumn("month",month(col("actual_date")))
      .withColumn("day", dayofmonth(col("actual_date")))
      .filter(col("year")==="2016")
      .filter(col("month")==="09")
      .filter(col("day")==="13")
      .select(
        lit(17).as("id"),
        col("arrival_city").as("response")
      ).distinct()
      .orderBy("response")


//    Question18

    val res18 = ticket_flights
      .join(flights, "flight_id")
      .filter(col("status")=!=("Cancelled"))
      .groupBy("flight_id")
      .agg(sum("amount").as("res"))
      .withColumn("num", rank().over(Window.orderBy(col("res").desc)))
      .filter(col("num")===1)
      .select(
        lit(18).as("id"),
        col("flight_id").as("response")
      )
      .orderBy("response")


//    Question19

    val res19 = flights_v
      .filter(col("actual_departure").isNotNull)
      .filter(col("status")=!=("Cancelled"))
      .withColumn("actual_date", to_utc_timestamp(col("actual_departure"), "Europe/Moscow"))
      .withColumn("date_actual", col("actual_date").cast(DateType))
      .groupBy(col("date_actual"))
      .agg(count("flight_id").as("count_f"))
      .withColumn("num", rank().over(Window.orderBy(col("count_f"))))
      .filter(col("num")===1)
      .select(
        lit(19).as("id"),
        col("date_actual").as("response")
      )
      .orderBy("response")


//    Question20

    val res20 = flights_v
      .filter(col("status").isin("Arrived", "Departed"))
      .filter(col("departure_city")===("Москва"))
      .withColumn("actual_date", to_utc_timestamp(col("actual_departure"), "Europe/Moscow"))
      .withColumn("year", year(col("actual_date")))
      .withColumn("month",month(col("actual_date")))
      .withColumn("day", dayofmonth(col("actual_date")))
      .filter(col("year")==="2016")
      .filter(col("month")==="09")
      .agg((count("flight_id")/30).cast("int").as("response"))
      .select(
        lit(20).as("id"),
        col("response")
      )



//    Question21

    val res21 = flights_v
      .withColumn("duration", unix_timestamp(col("actual_duration"), "HH:mm:ss"))
      .groupBy("departure_city")
      .agg((avg("duration")/3600).as("res"))
      .filter(col("res")>3)
      .withColumn("num", rank().over(Window.orderBy(col("res").desc)))
      .filter(col("num")<6)
      .select(
        lit(21).as("id"),
        col("departure_city").as("response")
      )
      .orderBy("response")


    val DFFinal = res1.union(res2)
      .union(res3)
      .union(res4)
      .union(res5)
      .union(res6)
      .union(res7)
      .union(res8)
      .union(res9)
      .union(res10)
      .union(res11)
      .union(res12)
      .union(res13)
      .union(res14)
      .union(res15)
      .union(res16)
      .union(res17)
      .union(res18)
      .union(res19)
      .union(res20)
      .union(res21)
//
//
    DFFinal
      .write.mode("overwrite")
      .saveAsTable(RESULT_TABLE)

  }
}