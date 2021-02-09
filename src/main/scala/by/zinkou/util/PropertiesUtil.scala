package by.zinkou.util

import java.util.Properties

/**
 * Helper for working with app.properties
 * */
object PropertiesUtil {

  val KAFKA_TOPIC_PROPERTY: String = "kafka.topic"
  val KAFKA_BOOTSTRAP_SERVER_PROPERTY: String = "kafka.bootstrap.server"
  val KAFKA_STARTING_OFFSET_PROPERTY: String = "kafka.starting.offset"
  val HADOOP_EXPEDIA_FOLDER_PROPERTY: String = "hadoop.expedia.folder.url"
  val HADOOP_SAVING_FOLDER_PROPERTY_BATCH: String = "hadoop.saving.folder.batch.url"
  val HADOOP_SAVING_FOLDER_PROPERTY_STREAM: String = "hadoop.saving.folder.stream.url"
  val HADOOP_EXPEDIA_CHECKPOINTING_FOLDER_PROPERTY :String = "hadoop.expedia.folder.checkpointing.dir"

  val properties = new Properties()
  properties.load(this.getClass.getClassLoader.getResourceAsStream("app.properties"))

}
