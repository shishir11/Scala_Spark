package practise.scala.spark.dao.crud

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.JdbcRDD
import java.sql.{ Connection, DriverManager }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SQLContext;

import practise.scala.spark.dao.connection.SparkSqlClass;
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.Dataset

object OrderDataAnalysis {
 
 /* def  getDesireDataFrame(sqlContext: SQLContext, dbName: String, dbTable: String):Dataset={
    val dataFrame =  sqlContext.read.format("jdbc").option("url", dbName)
                 .option("dbtable", dbTable)
                 .option("user", "root")
                 .option("password", "root")
                 .load();
     
   return dataFrame.toDF();
  }*/
  
  def main(orderDataAnalysis: Array[String]){
    val sparkSqlClass = new SparkSqlClass();
    val sparkContext=sparkSqlClass.getSparkContext();
    val sqlContext = new SQLContext(sparkContext);
 
    val ordersDF = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sparkdb")
                 .option("dbtable", "orders")
                 .option("user", "root")
                 .option("password", "root")
                 .load();
   
   
    val orderItemsDF = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sparkdb")
                 .option("dbtable", "order_items")
                 .option("user", "root")
                 .option("password", "root")
                 .load();
   
    
    val rightJoinOnOrderAndOrdeItem = ordersDF.join(orderItemsDF,ordersDF("order_id")===orderItemsDF("order_item_order_id"));
    
    val resultOfRightJoinOrderAndOrdeItem = rightJoinOnOrderAndOrdeItem.select("order_date","order_status","order_item_quantity","order_item_subtotal").orderBy("order_date");
    
   resultOfRightJoinOrderAndOrdeItem.write.format("com.databricks.spark.csv").save("D:\\tmp\\orders.csv");
    
    
    
                                  
  }
}