package by.zinkou.util

import org.apache.spark.sql.types._

object Schemas {

  /**
   * expedia schema, allow read data in stream manner from hdfs
   * */
  val expediaSchema: StructType = StructType(
    StructField("id", LongType)
      :: StructField("srch_ci", StringType)
      :: StructField("srch_co", StringType)
      :: StructField("hotel_id", LongType)
      :: StructField("srch_children_cnt", IntegerType)
      :: Nil)


  /**
   * hotels schema, allow to parse json data
   * */
  val hotelsSchema: StructType = StructType(
    StructField("Id", LongType)
      :: StructField("Country", StringType)
      :: StructField("City", StringType)
      :: StructField("Address", StringType)
      :: StructField("Name", StringType)
      :: StructField("TemperatureRecords", ArrayType(
      StructType(
        StructField("Day", StringType)
          :: StructField("Temperature", DoubleType)
          :: Nil))
    )
      :: Nil)
}
