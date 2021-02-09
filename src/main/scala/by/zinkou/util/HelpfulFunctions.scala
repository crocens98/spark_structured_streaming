package by.zinkou.util

import by.zinkou.beans.{HotelIdPairs, TypeCount}
import org.apache.spark.sql.functions.udf

object HelpfulFunctions {

  /**
   * UDF for mapping state
   * */
  val typeCheckUDF = udf((param: Long) => {
    if (param == 1) {
      "Short stay"
    } else if (param >= 1 && param <= 7) {
      "Standart stay"
    } else if (param >= 8 && param <= 14) {
      "Standart extended stay"
    } else if (param >= 15 && param <= 30) {
      "Long stay"
    } else {
      "Erroneous data"
    }
  })

  /**
   * Build final result and calculate most_popular_stay_type
   *
   * */
  def buildResultRow(row: HotelIdPairs) = {
    val map = scala.collection.mutable.Map[String, Long]()
    var maxType: TypeCount = null;
    for (i <- row.pairs) {
      if (maxType == null || maxType.count < i.count) {
        maxType = i
      }
      map += (i.type_f -> i.count)
    }

    (row.hotel_id,
      //with_children column
      row.children_sum > 0,
      map.get("Erroneous data"),
      map.get("Short stay"),
      map.get("Standart stay"),
      map.get("Standart extended stay"),
      map.get("Long stay"),
      maxType)
  }
}
