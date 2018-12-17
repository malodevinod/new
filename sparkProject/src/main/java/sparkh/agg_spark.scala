package sparkh

import org.apache.spark.sql.SparkSession
import java.io.File

object agg_spark {
  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("/user/hive/warehouse/").getAbsolutePath
    val spark=SparkSession.builder()
    .appName("spark hive")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()
    val sc=spark.sparkContext
    val sqlContext=spark.sqlContext
    sqlContext.sql("show databases").show()
    sqlContext.sql("use testdb").show()
   val df= spark.sql("select * from testdb.pro_part")
    df.show(false)
    
  }
}