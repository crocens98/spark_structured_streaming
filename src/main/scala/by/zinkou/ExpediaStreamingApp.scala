package by.zinkou

import by.zinkou.beans.HotelIdPairs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import by.zinkou.util.HelpfulFunctions.{buildResultRow, typeCheckUDF}
import by.zinkou.util.PropertiesUtil._
import by.zinkou.util.Schemas.{expediaSchema, hotelsSchema}

object ExpediaStreamingApp {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkExpediaStreamApp")
      .getOrCreate();
    import spark.implicits._

    /**
     * Read expedia data in structured stream way
     */
    val expedia = spark
      .readStream
      .format("avro")
      .schema(expediaSchema)
      .load(s"${properties.getProperty(HADOOP_EXPEDIA_FOLDER_PROPERTY)}/year=2017")


    /**
     * Read hotels data, will be used in broadcast join
     */
    val kafka = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", properties.getProperty(KAFKA_BOOTSTRAP_SERVER_PROPERTY))
      .option("subscribe", properties.getProperty(KAFKA_TOPIC_PROPERTY))
      .option("startingOffsets", properties.getProperty(KAFKA_STARTING_OFFSET_PROPERTY))
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value").cast("string"), hotelsSchema) as "parsed_value")
      .select(col("parsed_value.id") as "id", explode(col("parsed_value.TemperatureRecords")) as "tr")
      .select("id", "tr.Day", "tr.Temperature")

    /**
     * Read Expedia data from HDFS for 2017 year in stream manner.
     * */
    val joinedData = expedia
      .join(broadcast(kafka), (expedia("hotel_id") <=> kafka("id")) && (expedia("srch_ci") <=> kafka("day")))
      .where($"Temperature" > 0)
      .withColumn("period", datediff(col("srch_co"), col("srch_ci")))
      .withColumn("type_f", typeCheckUDF(col("period")))
      .groupBy("hotel_id", "type_f")
      .agg(count(expedia("id")) as "count"
        , sum(col("srch_children_cnt")) as "children_sum")

    val query = joinedData.writeStream
      .option("checkpointLocation", properties.getProperty(HADOOP_EXPEDIA_CHECKPOINTING_FOLDER_PROPERTY))
      .outputMode(OutputMode.Complete())
      .foreachBatch((df, batchId) => {
        df.groupBy("hotel_id")
          .agg(collect_list(struct(col("type_f"), col("count"))) as "pairs"
            // Apply additional variance with filter on children presence in booking (with_children: false - children <= 0; true - children > 0).
            , sum("children_sum") as "children_sum")
          .as[HotelIdPairs]
          .map(buildResultRow)
          .toDF(
            "hotel_id",
            "with_children",
            "erroneous_data_cnt",
            "short_stay_cnt",
            "standart_stay_cnt",
            "standart_extended_stay_cnt",
            "long_stay_cnt",
            "most_popular_stay_type")
          .withColumn("batch_timestamp", current_timestamp())
          .write
          .format("avro")
          .save(properties.getProperty(HADOOP_SAVING_FOLDER_PROPERTY_STREAM))
      })
      .start()

    query.awaitTermination()

  }
}
