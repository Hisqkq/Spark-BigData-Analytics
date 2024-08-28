import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import de.l3s.concatgz.io.warc.{WarcGzInputFormat, WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf

case class ImageData(pageUrl: String, imageUrl: String, size: Int)

val sparkConf = new SparkConf()
  .setAppName("RUBigDataApp")
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .registerKryoClasses(Array(classOf[WarcRecord]))

val spark = SparkSession.builder.config(sparkConf).getOrCreate()
import spark.implicits._

val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

val warcFile = "/opt/hadoop/rubigdata/multilingualism.warc.gz"

val sc = spark.sparkContext

val warcs = sc.newAPIHadoopFile(
  warcFile,
  classOf[WarcGzInputFormat],             // InputFormat
  classOf[NullWritable],                  // Key
  classOf[WarcWritable]                   // Value
).cache()

// Filter for URLs that contain 'wikipedia.org'
val filteredWarcs = warcs.filter { case (_, wr) =>
  val header = wr.getRecord.getHeader
  header.getHeaderValue("WARC-Type") == "response" &&
    header.getUrl.contains("wikipedia.org")
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

// Search for the largest image in imageData
val largestImage = imageData.reduce((a, b) => if (a.size > b.size) a else b)

println(s"Total number of images: $numImages")
println(s"Mean arbitrary size: $meanArbitrarySize")
println(s"Largest image URL: ${largestImage.imageUrl} with size: ${largestImage.size}")
println(s"Webpage URL: ${largestImage.pageUrl}")

spark.stop()