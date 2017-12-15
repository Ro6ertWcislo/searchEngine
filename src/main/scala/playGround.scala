import scala.io.Source
import org.apache.spark.SparkContext

object playGround extends App {
//  def fileToBagOfWOrdsVector(file: String): Map[String, Int] = {
//    Source.fromFile(file)
//      .getLines()
//      .flatMap(_.split("\\W+"))
//      .map(_.toLowerCase)
//      .foldLeft(Map.empty[String, Int]) { (wordMap, word) => wordMap + (word -> (wordMap.getOrElse(word, 0) + 1)) }
//  }

//  def bagOfWords(files: Iterable[String]) = {
//    files.flatMap(file => Source.fromFile(file).getLines())
//      .flatMap(_.split("\\W+"))
//      .map(_.toLowerCase)
//      .toSet
//        .foreach(println)
//  }

  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.rdd.RDD


  @transient lazy val conf: SparkConf = new SparkConf()
    .setMaster("local").setAppName("SVDsearchEnginee")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  def fileToBagOfWOrdsVector(file: RDD[String]): RDD[(String, Int)] = {
    file
      .flatMap(_.split("\\W+"))
      .map(_.toLowerCase)
      .map { word => (word, 1) }
      .reduceByKey(_ + _)
  }

//  def bagOfWords(files: Iterable[String]) = {
//    files.flatMap(file => Source.fromFile(file).getLines())
//      .flatMap(_.split("\\W+"))
//      .map(_.toLowerCase)
//      .toSet
//      .foreach(println)
//  }


  val x = fileToBagOfWOrdsVector(sc.textFile("C:\\Users\\ezwciro\\IdeaProjects\\SVDsearchEngine\\arts\\*")).collect()
  x.foreach(println)
}
