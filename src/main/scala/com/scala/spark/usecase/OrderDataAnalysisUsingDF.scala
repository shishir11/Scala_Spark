package com.scala.spark.usecase

import org.apache.spark.sql.SQLContext
import practise.scala.spark.dao.connection.SparkSqlClass
import org.apache.spark.sql.DataFrame;

object OrderDataAnalysis {

  def getDesireDataFrame(sqlContext: SQLContext, dbName: String, dbTable: String): DataFrame = {
    val dataFrame = sqlContext.read.format("jdbc").option("url", dbName)
      .option("dbtable", dbTable)
      .option("user", "root")
      .option("password", "root")
      .load();

    return dataFrame.toDF();
  }

  def main(orderDataAnalysis: Array[String]) {
    val sparkSqlClass = new SparkSqlClass();
    val sparkContext = sparkSqlClass.getSparkContext();
    val sqlContext = new SQLContext(sparkContext);

    val ordersDF = getDesireDataFrame(sqlContext, "jdbc:mysql://localhost:3306/sparkdb", "orders");

    val orderItemsDF = getDesireDataFrame(sqlContext, "jdbc:mysql://localhost:3306/sparkdb", "order_items");

    val rightJoinOnOrderAndOrdeItem = ordersDF.join(orderItemsDF, ordersDF("order_id") === orderItemsDF("order_item_order_id"));

    val resultOfRightJoinOrderAndOrdeItem = rightJoinOnOrderAndOrdeItem.select("order_date", "order_status", "order_item_quantity", "order_item_subtotal").orderBy("order_date");
    
    resultOfRightJoinOrderAndOrdeItem.write.format("com.databricks.spark.csv").save("D:\\tmp\\orders.csv");

  }
}