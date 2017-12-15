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

val x = fileToBagOfWOrdsVector(sc.textFile("a.txt")).collect()
x.foreach(println)