package practise.scala.spark.certification

import org.apache.spark.sql.SQLContext

object CCA175CertlibraryPractise {
  
  def main(array: Array[String]): Unit = {
    val sc = new practise.scala.spark.dao.connection.Connection();
    val sqlContext = new SQLContext(sc.getSparkContext());
    val sparkSession = sqlContext.sparkSession;
    
   val file1df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/cca175file1.csv");
   file1df.show()
   
   val file2df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/cca175file2.csv");
   file2df.show()
   
   val df = file1df.union(file2df).select("col1")
   
   val rdd1 = file1df.rdd
   
  }
}