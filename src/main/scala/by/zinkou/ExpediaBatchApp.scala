package by.zinkou

import by.zinkou.beans.HotelIdPairs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import by.zinkou.util.PropertiesUtil.{HADOOP_EXPEDIA_FOLDER_PROPERTY, HADOOP_SAVING_FOLDER_PROPERTY_BATCH, KAFKA_BOOTSTRAP_SERVER_PROPERTY, KAFKA_STARTING_OFFSET_PROPERTY, KAFKA_TOPIC_PROPERTY, properties}
import by.zinkou.util.Schemas.hotelsSchema
import by.zinkou.util.HelpfulFunctions.{buildResultRow, typeCheckUDF}

object ExpediaBatchApp {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkExpediaBatchApp5")
      .getOrCreate();

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

    import spark.implicits._
    /**
     * Read hotels data in batch manner
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
     * Read Expedia data from HDFS for 2016 year in batch manner.
     * */
    val expedia = spark
      .read
      .format("avro")
      .load(s"${properties.getProperty(HADOOP_EXPEDIA_FOLDER_PROPERTY)}/year=2016/")

    /**
     * add average temperature at checkin (join with hotels+weaher data from Kafka topic)
     */
    kafka
      .join(broadcast(expedia), (expedia("hotel_id") <=> kafka("id")) && (expedia("srch_ci") <=> kafka("day")))
      // Filter incoming data by having average temperature more than 0 Celsius degrees.
      .where($"Temperature" > 0)
      // Calculate customer's duration of stay as days between requested check-in and check-out date.
      .withColumn("period", datediff(col("srch_co"), col("srch_ci")))
      .withColumn("type_f", typeCheckUDF(col("period")))
      // Map each hotel with multi-dimensional state consisting of record counts for each type of stay
      .groupBy("hotel_id", "type_f")
      .count()
      .groupBy("hotel_id")
      .agg(collect_list(struct(col("type_f"), col("count"))) as "pairs")
      .withColumn("children_sum", lit(0L: Long))
      .as[HotelIdPairs]
      .map(buildResultRow).toDF(
      "hotel_id",
      "with_children",
      "erroneous_data_cnt",
      "short_stay_cnt",
      "standart_stay_cnt",
      "standart_extended_stay_cnt",
      "long_stay_cnt",
      "most_popular_stay_type")
      .drop(col("with_children"))
      .withColumn("batch_timestamp", current_timestamp())
      .write
      .format("avro")
      .save(properties.getProperty(HADOOP_SAVING_FOLDER_PROPERTY_BATCH))

  }
}
