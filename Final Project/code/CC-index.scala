package org.rubigdata

import org.apache.hadoop.io.{NullWritable}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// Define the case class outside of the object
case class ImageData(pageUrl: String, imageUrl: String, size: Int)

object RUBigDataApp {
  def main(args: Array[String]) {

    // Overriding default settings
    val sparkConf = new SparkConf()
                      .setAppName("RUBigDataApp")
                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .registerKryoClasses(Array(classOf[WarcRecord]))

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    import spark.implicits._

    // Getting the FileSystem object
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val warcDirPath = "hdfs:///single-warc-segment" // set your path here

    val warcFilesInDir = fs.listStatus(new Path(warcDirPath))
      .map(_.getPath.toString)
      .toSet

    val df = spark.read
      .option("mergeSchema", "true")
      .option("enable.summary-metadata", "false")
      .option("enable.dictionary", "true")
      .option("filterPushdown", "true")
      .parquet("hdfs://gelre/cc-index-subset/subset=warc/")
    df.createOrReplaceTempView("ccindex")

    val athleticsDf = spark.sql("SELECT warc_filename " +
      "FROM ccindex " +
      "WHERE (content_mime_detected='text/html' OR content_mime_detected='application/xhtml+xml') " +
      "AND url LIKE '%wikipedia.org%' " +
      "AND url LIKE '%athletics%'")

    // Filter out warc files that are not present in warcDirPath
    val warcFiles = athleticsDf.as[String].collect()
      .filter(warcFilesInDir.contains)

    val sc = spark.sparkContext

    val warcs = sc.union(warcFiles.map(file => sc.newAPIHadoopFile(
      file,
      classOf[WarcGzInputFormat],             // InputFormat
      classOf[NullWritable],                  // Key
      classOf[WarcWritable]                   // Value
    ))).cache()

		val filteredWarcs = warcs.filter { case (_, wr) =>
		  val header = wr.getRecord.getHeader
		  header.getHeaderValue("WARC-Type") == "response" &&
		  header.getUrl.contains("wikipedia.org") &&
		  header.getUrl.contains("athletics")
		}

    val imageData = filteredWarcs.mapPartitions { iter =>
      iter.flatMap { case (_, wr) =>
        val content = wr.getRecord.getHttpStringBody
        val pageUrl = wr.getRecord.getHeader.getUrl
        val pattern = """(?i)<img[^>]*src=['"]([^'"]+)[^>]*\swidth\s*=\s*['"](\d+)['"][^>]*\sheight\s*=\s*['"](\d+)['"][^>]*>""".r
        pattern.findAllMatchIn(content).map { m =>
          val imageUrl = m.group(1)
          val height = m.group(2).toInt
          val width = m.group(3).toInt
          val arbitrarySize = height * width
          ImageData(pageUrl, imageUrl, arbitrarySize)
        }
      }
    }

    val numImages = imageData.count()
    val meanArbitrarySize = imageData.map(_.size).mean()
    val largestImage = imageData.reduce((a, b) => if (a.size > b.size) a else b)

    println(s"Total number of images: $numImages")
    println(s"Mean arbitrary size: $meanArbitrarySize")
    println(s"Largest image URL: ${largestImage.imageUrl} with size: ${largestImage.size}")
    println(s"Webpage URL: ${largestImage.pageUrl}")

    spark.stop()
  }
}