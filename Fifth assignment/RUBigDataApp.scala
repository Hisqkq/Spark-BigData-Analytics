package org.rubigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed


object RUBigDataApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
    import spark.implicits._ 
    spark.sparkContext.setLogLevel("WARN")
    val regex = "^([A-Z].+) ([A-Z].+) was sold for (\\d+)gp$"
    val socketDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val sales = socketDF
      .select(
        regexp_extract($"value", regex, 2) as "tpe",
        regexp_extract($"value", regex, 3).cast(IntegerType) as "price",
        regexp_extract($"value", regex, 1) as "material"
      )
      .as[RuneData]

    sales.createOrReplaceTempView("sales")
    val item9850 = spark.sql("SELECT tpe, material, price " +
      "FROM sales " +
      "WHERE price >= 9840 AND price <= 9850 " +
      "GROUP BY tpe, material, price " +
      "HAVING price = 9850")


    val query = item9850
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()


    query.awaitTermination()
    spark.stop()
  }
}

case class RuneData(tpe: String, price: Int, material: String)


