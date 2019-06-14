package com.fjpiqueras.keepcoding.sparksql

import com.fjpiqueras.keepcoding.sparksql.SparkSQlEjerciciosV2X.User
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Ejercicio4 {

  case class products(crienteId: Int, tipo: String, precio: Double, stok: Int)

  case class transaction(fecha: String, hora: String, cliente: Int, productoId: String, itemAmount: String, totalPrice: Double)


  def main(args: Array[String]): Unit = {

    println("Hello, SparkSQL!")

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("SparkSQL - Keepcoding")
      .getOrCreate()

    import sparkSession.implicits._

  //  var prueba = sparkSession.read.text("file:///home/indizen/SparkScalaModule-ala/src/main/resources/input/products/products.txt").show


    val tabla1 = sparkSession.read.text("src/main/resources/input/products/products.txt")
      .map(row => row.toString().split("#"))
   .map(rowSplitted => products(rowSplitted(0).replace("[", "").toInt,
      rowSplitted(1),
      rowSplitted(2).toDouble,
      rowSplitted(3).replace("]", "").toInt))


    val tabla2 = sparkSession.read.text("src/main/resources/input/products/transactions.txt")
      .map(row => row.toString().split("#"))
      .map(rowSplitted => transaction(rowSplitted(0).replace("[", "").toString,
        rowSplitted(1),
        rowSplitted(2)toInt,
        rowSplitted(3),
        rowSplitted(4),
        rowSplitted(5).replace("]", "").toDouble))

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





    sparkSession.close()
  }
}
