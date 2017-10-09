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

ordersDF.registerTempTable("orders")
orderItemsDF.registerTempTable("orderitems")

val totalRevDaily = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal) as sumtotal from orders o join orderitems oi on o.order_id = oi.order_item_order_id where o.order_status = 'COMPLETE' group by o.order_date order by o.order_date")

totalRevDaily.rdd.saveAsTextFile(outpath)

    }
}
