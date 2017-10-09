package wordcount
import com.typesafe.config._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs._

object WordCount {
 def main(args: Array[String]) {
   val appconf = ConfigFactory.load()
   val conf = new SparkConf().
		setAppName("Word Count").
		setMaster(appconf.getString("deploymentMaster"))
	for(c <- conf.getAll)
            println(c._2)
        val sc = new SparkContext(conf)
        
        //val inPath = "/home/arjkan018304/data/wordcount.txt"
        //val outPath = "/home/arjkan018304/spark/WordCountImproved/output"

        val inPath = "/Users/urjun/spark/data/wordcount.txt"
        val outPath = "/Users/urjun/spark/WordCountImproved/output"

        val fs = FileSystem.get(sc.hadoopConfiguration)
        val inPathExists = fs.exists(new Path(inPath))
        val outPathExists = fs.exists(new Path(outPath))

        if(!inPathExists) {
            println("Invalid Input Path")
            return
        }

        if(outPathExists) {
            fs.delete(new Path(outPath), true)
        }

        val wc = sc.textFile(inPath).
                    flatMap(rec => rec.split(" ")).
                    map(rec => (rec, 1)).
                    reduceByKey((acc, value) => acc + value)
        wc.saveAsTextFile(outPath)
    }
}


