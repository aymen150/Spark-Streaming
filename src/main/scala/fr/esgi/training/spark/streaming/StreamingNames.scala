
package fr.esgi.training.spark.streaming

import org.apache.spark.sql.functions._
import fr.esgi.training.spark.utils.SparkUtils
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingNames {

  def main(args: Array[String]): Unit = {}
  val spark = SparkUtils.spark()
  import spark.implicits._

  //Créez un DataStream qui collecte la donnée à partir du Socket
  var df = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  val aggregateDF = df
  val aggregateDF1 = aggregateDF.withColumn("value_splited",  split(col("value"), ";"))

  /*
  En mode Append, seules les lignes ajoutées à la table de résultats depuis la dernière exécution de la requête sont
   présentes dans la table de résultats et écrites dans un stockage externe.
   */
  val aggregateDF2 = aggregateDF1.withColumn("tmp", col("value_splited")).select(
    col("tmp").getItem(0).as("sexe"),
    col("tmp").getItem(1).as("preusuel"),
    col("tmp").getItem(2).as("annais"),
    col("tmp").getItem(3).as("nombre").cast(DataTypes.IntegerType))

  aggregateDF2.writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination(3*60*100)

  //val premier_req = aggregateDF2.select(col("sexe"),col("nombre")).groupBy("sexe").sum("nombre")
  val DFsexe = aggregateDF2.select(col("sexe"),col("nombre")).groupBy("sexe").sum("nombre")
 /* DFsexe.writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination(10*100)
*/
  val DFmaxPrenom= aggregateDF2.select(col("preusuel"),col("nombre")).groupBy("preusuel").sum("nombre").orderBy(sum("nombre").desc)


  val DFannee = aggregateDF2.select(col("annais"),col("nombre")).groupBy("annais").sum("nombre").orderBy(sum("nombre").desc)

  // compter le nombre de naissance total


}

