package com.scala.spark.usecase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.catalyst.expressions.Ascending

object LogisticAnalytic {

  def printSchemaOfLogistic(sqlContext: SQLContext): Unit = {
    sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv").printSchema();
  }

  def showALL(sqlContext: SQLContext): Unit = {
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income").show();
  }

  def showALLWithSort(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income").sort(desc("Distance")).collect().foreach(println);
  }

  def sortUsingOrder(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.schema
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income").orderBy("Transport", "klients").show(10)
    import org.apache.spark.sql.functions._
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income").orderBy(asc("Department")).show(10)
  }

  def filterWitDistance(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income").filter(df("Distance") < 140000 && df("Weight") > 1000)
      .orderBy("Distance").collect().foreach(println);
  }

  def avgRecordUsingGroupBy(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income")
      .groupBy("Income").agg(avg("Income")).show();
  }

  def maxRedordUsingGroupBu(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income")
      .groupBy("Distance").agg(max("Distance")).show();
  }

  def minRedordUsingGroupBu(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income")
      .groupBy("Distance").agg(min("Distance")).show();
  }
  
  def sumRedordUsingGroupBu(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income")
      .groupBy("Income").agg("Distance" -> "sum").collect().foreach(println)
  }
  
  def countRedordUsingGroupBu(sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/Logistictrans.csv");
    df.select("Date", "Distance", "Weight", "Department", "Transport", "klients", "Income")
      .groupBy("Income").agg("Distance" -> "sum").collect().foreach(println)
  }
  
  def main(array: Array[String]): Unit = {
    val sc = new practise.scala.spark.dao.connection.Connection();
    val sqlContext = new SQLContext(sc.getSparkContext());

    avgRecordUsingGroupBy(sqlContext);

  }
}