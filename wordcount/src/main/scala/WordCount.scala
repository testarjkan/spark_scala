import org.apache.spark.SparkContext, org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    val inPath = "/Users/urjun/spark/data/wordcount.txt"
    val outPath = "/Users/urjun/spark/wordcount/output"
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster("local")
    val sc = new SparkContext(conf)
   // val inPath = args(0)
   // val outPath = args(1)
      
    val wc = sc.textFile(inPath).
      flatMap(rec => rec.split(" ")).
      map(rec => (rec, 1)).
      reduceByKey((acc, value) => acc + value)
      
    wc.map(rec => rec._1 + "," + rec._2).saveAsTextFile(outPath)

  }
}
