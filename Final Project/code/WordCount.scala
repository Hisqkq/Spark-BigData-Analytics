import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup

val spark = SparkSession.builder()
  .appName("WARC Analysis")
  .getOrCreate()

val warcData = spark.sparkContext.wholeTextFiles(warcfile)

// Extract the text content from the WARC data and remove HTML tags
val textContent = warcData.flatMap { case (_, content) =>
  val lines = content.split("\n")
  val header = lines.take(2) // Header lines
  val htmlContent = lines.drop(2).mkString("\n") // HTML content
  val doc = Jsoup.parse(htmlContent)
  val text = doc.text()
  val cleanedText = text.replaceAll("\\W+", " ") // Remove non-word characters except spaces
  cleanedText.split("\\s+")
}

val wordCount = textContent
  .map(word => (word.toLowerCase, 1))
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false)

wordCount.take(17).foreach(println)

spark.stop()