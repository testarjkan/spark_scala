package retail.dataframes

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object TotalRevDaily {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Total Reve Daily").setMaster("local")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._
        
        val inpath = "/Users/urjun/spark/data"
        val outpath = "/Users/urjun/spark/data/totalrevdaily/output"

        val ordersDF = sc.textFile(inpath + "/orders.csv").
                            map(_.split(",")).
                            map(x => Orders(x(0).toInt,
                                            x(1).toString,
                                            x(2).toInt,
                                            x(3).toString
                             )).toDF()

        val orderItemsDF = sc.textFile(inpath + "/order_items.csv").
                            map(_.split(",")).
                            map(x => OrderItems(x(0).toInt,
                                                x(1).toInt,
                                                x(2).toInt,
                                                x(3).toInt,
                                                x(4).toFloat,
                                                x(5).toFloat
                             )).toDF()


        val of = ordersDF.filter($"order_status" === "COMPLETE")

        //val oij = of.join(orderItemsDF, $"order_id" === $"order_item_order_id")

        val oij = of.join(orderItemsDF, of("order_id") === orderItemsDF("order_item_order_id") )

        oij.groupBy("order_date").
                  agg(sum("order_item_subtotal")).
                  sort("order_date").
                  rdd.map(x => (x(0) + "," + x(1))).
                  saveAsTextFile(outpath)
    }
}
