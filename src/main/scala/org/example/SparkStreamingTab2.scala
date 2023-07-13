package org.example

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.reflect.io.Directory
import java.io.File


object SparkStreamingTab2 extends App {
  val spark = SparkSession.builder
    .appName("Spark Streaming Tab2")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val bpmDataLocation = "C:\\Users\\sofia\\Desktop\\4iabd\\sparkStreamingScala\\src\\main\\ressources\\all_files_spark\\BPM\\*.csv"
  val activityLogLocation = "C:\\Users\\sofia\\Desktop\\4iabd\\sparkStreamingScala\\src\\main\\ressources\\activity_log\\*.json"

  // Schema definition would go here
  val activityLogSchema = defineActivityLogSchema()

  //  val bpmDF = spark.read.option("header", "true").csv(bpmDataLocation)
  val schema = defineSchema()
  val bpmDF = spark.readStream
    .option("maxFilesPerTrigger", 1)  // Adjust based on your requirement
    .option("header", "true")
    .schema(schema)
    .csv(bpmDataLocation)
  val activityLogDF = spark.readStream
    .option("maxFilesPerTrigger", 1)
    .schema(activityLogSchema)
    .json(activityLogLocation)

  val transformedDF = transformDataFrame(bpmDF, activityLogDF)

  val outputPath = "C:\\Users\\sofia\\Desktop\\4iabd\\sparkStreamingScala\\src\\main\\ressources\\all_files_spark\\BPM_plus_sport"

  val directory = new Directory(new File(outputPath))
  if (directory.exists) {
    directory.deleteRecursively()
  }

  val query = transformedDF.repartition(1).writeStream
    .outputMode("append")
    .format("csv")
    .option("truncate", false)
    .option("path", outputPath)
    .option("checkpointLocation", outputPath + "/checkpoint_csv")
    .trigger(Trigger.ProcessingTime("1 seconds"))
    .start()

  query.awaitTermination()

  def defineActivityLogSchema(): StructType = {
    // Define your schema here based on the JSON structure
    // e.g.
    val minutesInHeartRateZonesSchema = ArrayType(
      StructType(Seq(
        StructField("minuteMultiplier", LongType),
        StructField("minutes", LongType),
        StructField("order", LongType),
        StructField("type", StringType),
        StructField("zoneName", StringType)
      ))
    )

    // Active Zone Minutes Schema
    val activeZoneMinutesSchema = StructType(Seq(
      StructField("minutesInHeartRateZones", minutesInHeartRateZonesSchema),
      StructField("totalMinutes", LongType)
    ))

    // Activity Level Schema
    val activityLevelSchema = ArrayType(
      StructType(Seq(
        StructField("minutes", LongType),
        StructField("name", StringType)
      ))
    )

    // Heart Rate Zones Schema
    val heartRateZonesSchema = ArrayType(
      StructType(Seq(
        StructField("caloriesOut", DoubleType),
        StructField("max", LongType),
        StructField("min", LongType),
        StructField("minutes", LongType),
        StructField("name", StringType)
      ))
    )

    // Manual Values Specified Schema
    val manualValuesSpecifiedSchema = StructType(Seq(
      StructField("calories", BooleanType),
      StructField("distance", BooleanType),
      StructField("steps", BooleanType)
    ))

    // Source Schema
    val sourceSchema = StructType(Seq(
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("trackerFeatures", ArrayType(StringType)),
      StructField("type", StringType),
      StructField("url", StringType)
    ))

    // Overall Schema
    StructType(Seq(
      StructField("activeDuration", LongType),
      StructField("activeZoneMinutes", activeZoneMinutesSchema),
      StructField("activityLevel", activityLevelSchema),
      StructField("activityName", StringType),
      StructField("activityTypeId", LongType),
      StructField("averageHeartRate", LongType),
      StructField("calories", LongType),
      StructField("caloriesLink", StringType),
      StructField("distance", DoubleType),
      StructField("distanceUnit", StringType),
      StructField("duration", LongType),
      StructField("elevationGain", DoubleType),
      StructField("hasActiveZoneMinutes", BooleanType),
      StructField("heartRateLink", StringType),
      StructField("heartRateZones", heartRateZonesSchema),
      StructField("lastModified", StringType),
      StructField("logId", LongType),
      StructField("logType", StringType),
      StructField("manualValuesSpecified", manualValuesSpecifiedSchema),
      StructField("originalDuration", LongType),
      StructField("originalStartTime", StringType),
      StructField("pace", DoubleType),
      StructField("source", sourceSchema),
      StructField("speed", DoubleType),
      StructField("startTime", StringType),
      StructField("steps", LongType),
      StructField("tcxLink", StringType)
    ))
  }

  def defineSchema(): StructType = {
    StructType(Array(
      StructField("time", StringType, true),
      StructField("value", IntegerType, true),
      StructField("date", StringType, true),
      StructField("HOUR", StringType, true)
    ))
  }

  def transformDataFrame(bpmDF: DataFrame, activityLogDF: DataFrame): DataFrame = {

    val df2 = activityLogDF.select(col("originalDuration").alias("Duration"),
      col("originalStartTime").alias("startTime"),
      col("activityName").alias("activityName"))
      .withColumn("date", to_timestamp(split(col("startTime"), "T")(0), "yyyy-MM-dd"))
      .withColumn("HOUR", split(col("startTime"), "T")(1))
      .withColumn("StartTime", unix_timestamp(col("startTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").cast("timestamp"))
      .withColumn("EndTime", (unix_timestamp(col("startTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") + col("Duration") / 1000).cast("timestamp"))
      .withColumn("Duration", expr("Duration/1000"))
      .withColumn("timeList", expr("sequence(StartTime, EndTime, interval 1 minute)"))
      .withColumn("timeList", explode(col("timeList")))
      .select(col("date"), concat(date_format(col("timeList"), "HH:mm"), lit(":00")).alias("time"), col("activityName"))

    val bpmDFWithDateAsTimestamp = bpmDF.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd"))

    val bpmDFWithWatermark: DataFrame = bpmDFWithDateAsTimestamp.withWatermark("date", "1 seconds")
    val df2WithWatermark: DataFrame = df2.withWatermark("date", "2 seconds")

    bpmDFWithWatermark.join(
      df2WithWatermark,
      Seq("date", "time"),
      "left_outer"
    )
      .withColumn("sport", when(col("activityName").isNotNull, col("activityName")).otherwise("0"))
      .withColumn("TS", concat(date_format(col("date"), "yyyy-MM-dd"), date_format(col("time"), "HH:mm:ss")))
      .select(col("sport"), col("TS"), col("time"), col("value"), col("date"), col("HOUR"))
  }

}
