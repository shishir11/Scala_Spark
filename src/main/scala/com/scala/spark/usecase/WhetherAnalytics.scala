package com.scala.spark.usecase

import org.apache.spark.sql.SQLContext

object WhetherAnalytics {
  
  def main(arr: Array[String]): Unit = {
    val sc = new practise.scala.spark.connection.Connection();
    val sqlContext = new SQLContext(sc.getSparkContext());
    val sparkSession = sqlContext.sparkSession;
    
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("D://Project//scala//ratings//whetherhistory.csv");

    println("Printing Schema.");
    df.printSchema();
    
    df.select("Month","Day","Hour","Minute").take(100).foreach(println);
    
    val result = df.select("Month","Day","Hour","Minute");
    sc.getSparkContext().parallelize(Seq(result),1);
  }
}