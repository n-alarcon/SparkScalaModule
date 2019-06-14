package com.fjpiqueras.keepcoding.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkSQlEjerciciosV2X {

  case class User (usuarioId: Int, genero: String, edad: Int, ocupacion: String, codigopostal: String)
  case class Rating (usuarioId: Int, peliculaId: Int, nota: Int, tm: Long)
  case class Pelicula (peliculaId: Int, pelicula: String, genero: String)

  def main(args: Array[String]): Unit = {

    println("Hello, SparkSQL!")

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("SparkSQL - Keepcoding")
      .getOrCreate()

    import sparkSession.implicits._


    //Ejercicio 1: Mapea el dataset users.dat en su case class correspondiente
    val usuarios = sparkSession.read.text("file:///home/indizen/SparkScalaModule-ala/dataset/users.dat").map(row => row.toString().split("::"))
      .map(rowSplitted => User(rowSplitted(0).replace("[", "").toInt, rowSplitted(1), rowSplitted(2).toInt, rowSplitted(3), rowSplitted(4).replace("]", "")))


    //Ejercicio 2: Mapea el dataset ratings.dat en su case class correspondiente
    val notas = sparkSession.read.text("file:///home/indizen/SparkScalaModule-ala/dataset/ratings.dat")
      .map(row => row.toString().split("::")).map(rowSplitted =>
      Rating(rowSplitted(0).replace("[", "").toInt, rowSplitted(1).toInt, rowSplitted(2).toInt, rowSplitted(3).replace("]", "").toInt))

    //Ejercicio 3: Mapea el dataset movies.dat en su case class correspondiente
    val peliculas = sparkSession.read.text("file:///home/indizen/SparkScalaModule-ala/dataset/movies.dat")
      .map(row => row.toString().split("::")).map(rowSplitted =>
      Pelicula(rowSplitted(0).replace("[", "").toInt, rowSplitted(1), rowSplitted(2).replace("]", "").toString))

    //Ejercicio 3: Muestra la información de ambos datasets

    usuarios.show
    notas.show
    peliculas.show

    //Ejercicio 4: Muestra el esquema de la tablas
    usuarios.printSchema()
    notas.printSchema()
    peliculas.printSchema()

    //Ejercicio 5: Crea tablas temporales sobre los dos anteriores dataframes
    usuarios.createOrReplaceGlobalTempView("USUARIOS")
    notas.createOrReplaceGlobalTempView("NOTAS")
    peliculas.createOrReplaceGlobalTempView("PELICULAS")

    //Ejercicio 6: Calcula la nota media que ha realizado cada usuario
    val ejercicio6 = sparkSession.sql("SELECT u.usuarioId, AVG(nota) FROM global_temp.USUARIOS u JOIN global_temp.NOTAS r ON (u.usuarioId = r.usuarioId) GROUP BY u.usuarioId")

    ejercicio6.show(10)

    //Ejercicio 7: Muestra la nota de cada pelicula
    val ejercicio7 = sparkSession.sql("SELECT peli.peliculaId, peli.pelicula, nota.nota FROM global_temp.PELICULAS peli JOIN global_temp.NOTAS nota ON (peli.peliculaId = nota.peliculaId)")

    ejercicio7.show(10)

    // Muestra 10 registros de global_temp.PELICULAS
    val varios8 = sparkSession.sql("SELECT * FROM global_temp.PELICULAS ")
    varios8.show(10)

    // Muestra 20  registros de global_temp.PELICULAS ordenados por Descendente por genero
    val varios9 = sparkSession.sql("SELECT * FROM global_temp.PELICULAS ORDER BY genero DESC LIMIT 20")
    varios9.show(20)

    // Cuenta todos llas lineas de la tabla  global_temp.PELICULAS
    val Varios10 = sparkSession.sql("select count(*) from global_temp.PELICULAS")
    Varios10.show(10)

    // muestra solo dos tablas de la  global_temp.PELICULAS
    val Varios11 = sparkSession.sql("select peliculaId, pelicula from global_temp.PELICULAS ")
    Varios11.show(10)

    // muestra 20 usuarios con codigo postal 55113
    val Varios12 = sparkSession.sql("SELECT usuarioId, genero, edad, ocupacion, codigopostal  from global_temp.USUARIOS WHERE codigopostal='55113' ")
    Varios12.show(20)

    // muestra 20 usuarios con codigo postal 55113 pero esta vez mujeres
    val Varios13 = sparkSession.sql("SELECT usuarioId, genero, edad, ocupacion, codigopostal  from global_temp.USUARIOS WHERE codigopostal='55113' AND genero='F' ")
    Varios13.show(20)

    // muestra 20 usuarios con codigo postal 55113 pero esta vez mujeres, ordenadas de forma descendente
    val Varios14 = sparkSession.sql("SELECT usuarioId, genero, edad, ocupacion, codigopostal  from global_temp.USUARIOS WHERE codigopostal='55113' AND genero='F' ORDER BY usuarioId desc")
    Varios14.show(20)

    // muestra 20 usuarios con codigo postal 55113 pero esta vez mujeres, ordenadas de forma descendente, que tengan mas de 36 años
    val Varios15 = sparkSession.sql("SELECT usuarioId, genero, edad, ocupacion, codigopostal  from global_temp.USUARIOS WHERE codigopostal='55113' AND genero='F' AND edad > 36 ORDER BY usuarioId desc")
    Varios15.show(20)

    //
    val Varios16 = sparkSession.sql("SELECT usuarioId, genero, edad, ocupacion, codigopostal  from global_temp.USUARIOS WHERE codigopostal='55113' AND genero='F' AND edad > 36 ORDER BY usuarioId desc")
    Varios16.show(20)

    // muestra tablas usuarioId,Pelicula y notas
    val Varios17 = sparkSession.sql("SELECT usuarioId, pelicula, nota FROM global_temp.PELICULAS JOIN global_temp.NOTAS ON (PELICULAS.peliculaId = NOTAS.peliculaId) ")
    Varios17.show(20)

    // muestra tablas usuarioId,Pelicula y notas por media ordenados por descendente por nota
    val Varios18 = sparkSession.sql(
      "SELECT usuarioId, pelicula, AVG(nota) nota " +
        "FROM global_temp.PELICULAS JOIN global_temp.NOTAS ON (PELICULAS.peliculaId = NOTAS.peliculaId) " +
        "GROUP BY usuarioId, pelicula ORDER BY nota desc")
    Varios18.show(20)


    // muestra tablas usuarioId,Pelicula y notas por media ordenados por descendente por nota
    val Varios19 = sparkSession.sql(
      "SELECT usuarioId, pelicula, AVG(nota) nota " +
        "FROM global_temp.PELICULAS JOIN global_temp.NOTAS ON (PELICULAS.peliculaId = NOTAS.peliculaId) " +
        "GROUP BY usuarioId, pelicula ORDER BY nota desc ")
    Varios19.show(20)



    sparkSession.close()
  }
}
