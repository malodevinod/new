package sparkh

import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
   val spark=SparkSession.builder()
   .appName("vinod")
   .enableHiveSupport()
   .getOrCreate()
    val sc=spark.sparkContext
    val sqlContext=spark.sqlContext
   //product_id,purchase_date,ap,cp,sp,country_name
    import sqlContext.implicits._
    
      
    val offset_table=args(0)
    val pro_cat_table=args(1)
    val pro_infod=args(2)
  val pdf=sc.textFile(pro_infod).map(_.split(","))
  .map(x=>(x(0),x(1),x(2),x(3),x(4),x(5))).toDF("product_id","purchase_date","ap","cp","sp","country_name")
  .where("product_id!='product_id'")
  //  pdf.show()
    
    //prod_id,prod_name,pro_cat
    val p_catdf=sc.textFile(pro_cat_table).map(_.split(","))
    .map(x=>(x(0),x(1),x(2))).toDF("prod_id","prod_name","pro_cat").where("prod_id!='prod_id'")
  //p_catdf.show()
    
    
    val confdf=sc.textFile(offset_table)
    .map(x=>x.split(",")).map(x=>(x(0),x(1),x(2),x(3)))
.toDF("country_name","start_time","end_time","offset")
.where("country_name !='country_name'")
//confdf.show()

  val join_p_c= pdf.join(p_catdf,p_catdf.col("prod_id")===pdf("product_id"))


val joinDf=join_p_c.join(confdf,join_p_c.col("country_name")===confdf.col("country_name"))
.where(confdf.col("start_time")<join_p_c.col("purchase_date") && confdf.col("end_time")>join_p_c.col("purchase_date"))
.select(join_p_c("product_id"),join_p_c("purchase_date")
    ,join_p_c("ap"),join_p_c("cp"),join_p_c("sp"),join_p_c("country_name"),p_catdf("prod_name"),p_catdf("pro_cat"),
    confdf("offset"))
 joinDf.show();

   def offset(offset:String):String={
      var a=(offset.toFloat)/60
        if(a<10)
{
           var f:Array[String]=(a.toString).split("\\.")
             if(f(1).length>=2)
              {
           var rslt1="0"+f(0)+":"+f(1).substring(0,2)
           return rslt1
               }
                 else
                 {
                   var rslt1="0"+f(0)+":"+f(1)+"0"
                   return rslt1
                 }
}
        else
{
            var ff:Array[String]=(a.toString).split("\\.")
              if(ff(1).length>=2)
                {
                 var rslt2=ff(0)+":"+ff(1).substring(0,2)
                 return rslt2
                 }
              
               else
                  {
                    var rslt11=ff(0)+":"+ff(1)+"0"
                    return rslt11

                   }
       }
}


val UDFoffset=spark.udf.register("offset",offset _)


import org.apache.spark.sql.functions._

val finalDf=joinDf.select(joinDf("product_id"),joinDf("ap"),joinDf("cp"),joinDf("sp")
    ,joinDf("country_name"),joinDf("prod_name"),joinDf("pro_cat"),joinDf("offset")
,concat($"purchase_date",concat(lit("+"),UDFoffset(substring($"offset",2,3)))).alias("purchase_date"))
finalDf.show(false)

val dateDf=finalDf.withColumn("date_part", to_date($"purchase_date")).
 select(finalDf("product_id"),finalDf("ap"),finalDf("cp"),finalDf("sp"),finalDf("country_name"),finalDf("prod_name"),finalDf("pro_cat"),finalDf("purchase_date"),regexp_replace($"date_part","-","").alias("date_part"))
 dateDf.show(false)
 
  dateDf.createTempView("tempview")
  spark.conf.set("hive.exec.dynamic.partition",true)
  spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
  
  /*create table pp_table
  (
      product_id string,
    ap Double,
    cp Double,
    sp Double,
    country_name string,
    prod_name string,
    pro_cat string,
    purchase_date string
  )
  partitioned by (date_part string)
  row format delimited 
  fields terminated by ','
  stored as Parquet;
  
  */
  sqlContext.sql("insert into testdb.pp_table partition (date_part) select * from tempview")
    
  }
}