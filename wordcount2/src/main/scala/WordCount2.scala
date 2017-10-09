import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount2 {
    def main(args: Array[String]) {
        val inpath = "/Users/urjun/spark/data/wordcount.txt"
        val outpath = "/Users/urjun/spark/data/wordcount2/output"

        val conf = new SparkConf().setAppName("word count").setMaster("local")
        val sc = new SparkContext(conf)

        val wc = sc.textFile(inpath)
        val wc1 = wc.flatMap(_.split(" ")).map((_, 1))
        val wc2 = wc1.reduceByKey((acc, value) => acc + value)
        wc2.collect().foreach(println)
	wc2.saveAsTextFile(outpath)
    }
}
