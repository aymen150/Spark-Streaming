package fr.esgi.training.spark.streaming

import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.types.DataTypes
import java.time.LocalDateTime
import org.apache.spark.sql.functions.window
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import fr.esgi.training.spark.utils.SparkUtils

import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.{Encoders, SparkSession}

import org.apache.spark.sql.types.DataTypes

import scala.io.StdIn
import java.sql.Timestamp
object StreamingIot {

  def main(args: Array[String]): Unit = {}

  val spark = SparkUtils.spark()

  import spark.implicits._

  var df = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .option("includeTimestamp", true)
    .load()
    .selectExpr("CAST(value AS STRING)", "timestamp")

  df.writeStream
      .where(col("") =!= "x")
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()

  /*
    var aggregateDF = df.withColumn("timestamp", col("timestamp"))
    aggregateDF = aggregateDF.withColumn("id_iot", split(col("value"), ";").getItem(0))
    aggregateDF = aggregateDF.withColumn("temp", split(col("value"), ";").getItem(1).cast("Float"))
    aggregateDF = aggregateDF.withColumn("sensors", split(col("value"), ";").getItem(3))



       En mode Complet, l’intégralité de la table de sortie est actualisée à chaque déclencheur. Autrement dit, la table
       inclut non seulement les données issues de l’exécution du dernier déclencheur, mais également les données de toutes
       les autres exécutions.
       */

  //withWatermark(ch oisir entre -> [value, timestamp, value_splited] , durée -> "10 minutes") <== Permet de définir le retard accepté de livraison d'une donnée
  // window(col("timestamp"),"1 minute")

  /*
   var aggregateDFiot1 = aggregateDF.withWatermark("timestamp", "10 minutes")
     .groupBy(col("id_iot"), window(col("timestamp"),"1 minute"))
     .mean("temp")

   aggregateDFiot1.writeStream
     .outputMode("complete")
     .format("console")
     .start()
     .awaitTermination()

   //2
   var aggregateDFiot2 = aggregateDF.withWatermark("timestamp", "10 minutes")
     .groupBy(col("id_iot"), window(col("timestamp"), "1 minute", "30 seconds"))
     .mean("temp")

   aggregateDFiot2.writeStream
     .outputMode("complete")
     .format("console")
     .start()
     .awaitTermination()

   //4

   var aggregateDFiot4 = aggregateDF.withWatermark("timestamp", "10 minutes")
     .groupBy(window(col("time"), "1 hour")).agg(min(col("temp")), avg(col("temp")), max(col("temp")))

   aggregateDFiot4.writeStream
     .outputMode("complete")
     .format("console")
     .start()
     .awaitTermination()
 */
}
