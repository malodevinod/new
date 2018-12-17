package sparkh.aggri

import org.apache.spark.sql.SparkSession
import java.io.File
object aggri {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("/user/hive/warehouse/").getAbsolutePath
    val spark=SparkSession.builder()
    .appName("spark hive")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()
    val sc=spark.sparkContext
    val sqlContext=spark.sqlContext
    //sqlContext.sql("show databases").show()
    //sqlContext.sql("use testdb").show()
   val df= spark.sql("select * from testdb.pro_part")
   
    df.show(false)
  /*df.show
+----------+-------+-------+------+------------+-----------+--------+--------------------+---------+
|product_id|     ap|     cp|    sp|country_name|  prod_name| pro_cat|       purchase_date|date_part|
+----------+-------+-------+------+------------+-----------+--------+--------------------+---------+
|        p5| 658.75| 1000.0| 700.0|       China|       dish| kitchen|2018-12-24T16:20:...| 20181224|
|        p1|1951.75| 2500.0|2200.0|       China|  headphone|     ele|2018-12-24T14:01:...| 20181224|
|        p5| 658.75| 1000.0| 700.0|       India|       dish| kitchen|2018-12-24T16:20:...| 20181224|
|        p1|1951.75| 2500.0|2200.0|       India|  headphone|     ele|2018-12-24T14:01:...| 20181224|
|        p4| 5000.0| 6000.0|5500.0|         USA|      jeans|   cloth|2018-12-15T10:25:...| 20181215|
|        p4| 5000.0| 6000.0|5500.0|         USA|      jeans|   cloth|2018-12-15T10:25:...| 20181215|
|        p3|1985.75| 2700.0|2500.0|         USA|     mobile|     ele|2018-12-02T10:05:...| 20181202|
|        p3|1985.75| 2700.0|2500.0|         USA|     mobile|     ele|2018-12-02T10:05:...| 20181202|
|        p6|2548.75| 3000.0|2700.0|       China|coconut_oil|  beauty|2018-12-12T12:05:...| 20181212|
|        p2|1985.75| 2600.0|2300.0|       India|  antivirus|software|2018-12-20T14:05:...| 20181220|
|        p6|2548.75| 3000.0|2700.0|         USA|coconut_oil|  beauty|2018-12-12T12:05:...| 20181212|
|        p2|1985.75| 2600.0|2300.0|         USA|  antivirus|software|2018-12-20T14:05:...| 20181220|
|        p3|1985.75| 2700.0|2500.0|       China|     mobile|     ele|2018-12-02T10:05:...| 20181202|
|        p7|6589.75|10000.0|7000.0|       India|         TV|     ele|2018-12-03T16:05:...| 20181203|
|        p7|6589.75|10000.0|7000.0|         USA|         TV|     ele|2018-12-03T16:05:...| 20181203|
+----------+-------+-------+------+------------+-----------+--------+--------------------+---------+
    
  */ 
    
    //import org.apache.spark.sql.expressions.Window 
   // df.groupBy(windo, cols)
   import org.apache.spark.sql.functions._
    df.groupBy(col("purchase_date"),window(col("purchase_date"),"30 day")).agg(sum("price")).show(false)
    df.groupBy(col("purchase_date"),window(col("purchase_date"),"15 minute")).agg(sum("price")).show(false)
    df.groupBy(col("purchase_date"),window(col("purchase_date"),"1 week")).agg(sum("price")).show(false)
    df.groupBy(col("purchase_date"),window(col("purchase_date"),"1 day")).agg(sum("price")).show(false)
    
  }
}