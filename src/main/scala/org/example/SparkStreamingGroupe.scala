package org.example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkStreamingGroupe extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  val inputPath = "C:\\Users\\sofia\\Desktop\\4iabd\\sparkStreamingScala\\src\\main\\ressources\\all_files_spark\\BPM\\*.csv"
  val outputPath1 = "C:\\Users\\sofia\\Desktop\\4iabd\\sparkStreamingScala\\src\\main\\ressources\\all_files_spark\\group_by_days"
  val outputPath2 = "C:\\Users\\sofia\\Desktop\\4iabd\\sparkStreamingScala\\src\\main\\ressources\\all_files_spark\\group_by_days_hour"

  // Delete the directories and their contents if they exist
  val directories = Seq(outputPath1, outputPath2).map(path => new java.io.File(path))
  directories.foreach { dir =>
    if (dir.exists) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }

  def defineSchema(): StructType = {
    StructType(Array(
      StructField("hour", StringType, true),
      StructField("value", IntegerType, true),
      StructField("date", StringType, true),
      StructField("id", StringType, true)
    ))
  }

  def calculateAndWrite(df: DataFrame, groupByColumns: Seq[String], outputPath: String): Unit = {
    val result = df.groupBy(groupByColumns.map(col): _*)
      .agg(min("value").alias("min_value"),
        avg("value").alias("avg_value"),
        max("value").alias("max_value"))

    println(s"Sortie (${groupByColumns.mkString(", ")}): ")
    result.show()
    result.printSchema()
    result.coalesce(1).write.option("header", "true").csv(outputPath)  // Coalesce the DataFrame into 1 partition before writing
  }

  def processBatch(df: DataFrame): Unit = {
    try {
      calculateAndWrite(df, Seq("date"), outputPath1)
      calculateAndWrite(df, Seq("date", "hour"), outputPath2)
    } catch {
      case e: Exception => println(e)
    }
  }

  def stat(spark: SparkSession): Unit = {
    val schema = defineSchema()

    val streamingDF = spark.readStream
      .option("header", "true")
      .schema(schema)
      .csv(inputPath)

    val query = streamingDF.writeStream
      .foreachBatch((df: DataFrame, _: Long) => processBatch(df))
      .start()

    query.awaitTermination()
  }

  stat(spark)
}
