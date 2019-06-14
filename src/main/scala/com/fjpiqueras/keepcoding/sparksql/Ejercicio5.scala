package com.fjpiqueras.keepcoding.sparksql

import com.fjpiqueras.keepcoding.sparksql.SparkSQlEjerciciosV2X.User
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ejercicio5 {

    case class hoteles( id: Int, name: String, address: String, zip: String, city_hotel: String, cc1: String, ufi: String, classs: String, currencycode: String, minrate: String, maxrate: String, preferred: String, nr_rooms: String, public_ranking: String, hotel_url: String, city_unique: String, city_preferred: String, review_score: String, review_nr: Int)
//    case class transaction(fecha: String, hora: String, cliente: Int, productoId: String, itemAmount: String, totalPrice: Double)


    def main(args: Array[String]): Unit = {

      println("Hello, SparkSQL!")

      val sparkSession = SparkSession.builder.
        master("local")
        .appName("SparkSQL - Keepcoding")
        .getOrCreate()

      import sparkSession.implicits._

      //  var prueba = sparkSession.read.text("file:///home/indizen/SparkScalaModule-ala/src/main/resources/input/products/products.txt").show


      val tabla1 = sparkSession.read.text("file:///home/indizen/SparkScalaModule-ala/src/main/resources/input/hoteles/hoteleseuropa.csv")
        .map(row => row.toString().split(";"))
        .map(rowSplitted => hoteles(rowSplitted(0).replace("[", "").toInt,
     //     .map(rowSplitted => hoteles(rowSplitted(0).toInt,
          rowSplitted(1),
          rowSplitted(2),
          rowSplitted(3),
          rowSplitted(4),
          rowSplitted(5),
          rowSplitted(6),
          rowSplitted(7),
          rowSplitted(8),
          rowSplitted(10),
          rowSplitted(11),
          rowSplitted(12),
          rowSplitted(13),
          rowSplitted(14),
          rowSplitted(15),
          rowSplitted(16),
          rowSplitted(17),
          rowSplitted(18),
          rowSplitted(19).replace("]", "").toInt)).show
    //      rowSplitted(19).toInt)).show



/*
      tabla1.show
      tabla2.show

      tabla1.printSchema()
      tabla2.printSchema()

      tabla1.createOrReplaceGlobalTempView("PRODUCTS")
      tabla2.createOrReplaceGlobalTempView("TRANSACTION")

      //  val num1 = transaction
      //      .groupBy("clienteId")
      //      .agg(round(sum("totalPrice"),2).as("totalPrice"), sum("itemAmount").as("itemAmount"))
      //      .jion(products.withColumnRenamed("id", "producto"))

      //  val topStockedProducts = pr

      //  val top = transaction
      //      .join(topS)



*/

      sparkSession.close()
    }
  }


