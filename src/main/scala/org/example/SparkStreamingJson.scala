package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import scala.reflect.io.Directory

import java.io.File

object SparkStreamingJson extends App {

  val spark = SparkSession.builder
    .appName("Spark Streaming Json")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Define schema for nested JSON structure
  val schema = defineSchema()

  // JSON file location
  val jsonFileLocation = "C:\\Users\\sofia\\Desktop\\4iabd\\sparkStreamingScala\\src\\main\\ressources\\activities_heart_streaming\\*.json"

  // Read JSON files from the specified location
  val jsonDF = spark.readStream
    .option("maxFilesPerTrigger", 1)
    .schema(schema)
    .json(jsonFileLocation)

  // Transform DataFrame
  val transformedDF = transformDataFrame(jsonDF)

  // Output path
  val outputPath = "C:\\Users\\sofia\\Desktop\\4iabd\\sparkStreamingScala\\src\\main\\ressources\\all_files_spark\\BPM"

  // Delete the BPM directory and its contents if it exists
  val directory = new Directory(new File(outputPath))
  if (directory.exists) {
    directory.deleteRecursively()
  }

  // Start the streaming query and write output to CSV
  val query = transformedDF.writeStream
    .outputMode("append")
    .format("csv")
    .option("truncate", false)
    .option("path", outputPath)
    .option("checkpointLocation", outputPath + "\\checkpoint_csv")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

  query.awaitTermination()

  def defineSchema(): StructType = {
    // Heart rate zones schema
    val heartRateZonesSchema = ArrayType(
      StructType(Seq(
        StructField("caloriesOut", DoubleType),
        StructField("max", IntegerType),
        StructField("min", IntegerType),
        StructField("minutes", IntegerType),
        StructField("name", StringType)
      ))
    )

    // Dataset schema
    val datasetSchema = ArrayType(
      StructType(Seq(
        StructField("time", StringType),
        StructField("value", IntegerType)
      ))
    )

    // Value schema
    val valueSchema = StructType(Seq(
      StructField("customHeartRateZones", ArrayType(StringType)),
      StructField("heartRateZones", heartRateZonesSchema),
      StructField("restingHeartRate", IntegerType)
    ))

    // Activities heart intraday schema
    val activitiesHeartIntradaySchema = StructType(Seq(
      StructField("dataset", datasetSchema),
      StructField("datasetInterval", IntegerType),
      StructField("datasetType", StringType)
    ))

    // Activities heart schema
    val activitiesHeartSchema = ArrayType(
      StructType(Seq(
        StructField("dateTime", StringType),
        StructField("value", valueSchema)
      )))

    // Overall schema
    StructType(Seq(
      StructField("activities-heart", activitiesHeartSchema),
      StructField("activities-heart-intraday", activitiesHeartIntradaySchema)
    ))
  }

  def transformDataFrame(df: DataFrame): DataFrame = {
    df.select(
      explode(col("activities-heart")).as("activities_heart"),
      col("activities-heart-intraday.dataset").as("dataset")
    ).select(
      col("activities_heart.dateTime").as("date"),
      explode(col("dataset")).as("vals")
    ).select(
      col("vals.time"),
      col("vals.value"),
      col("date"),
      split(col("vals.time"), ":").getItem(0).as("hour")
    )
  }
}
