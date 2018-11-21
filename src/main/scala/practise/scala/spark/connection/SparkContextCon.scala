package practise.scala.spark.connection

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class Connection {
  def getSparkContext(): SparkContext = {
      System.setProperty("hadoop.home.dir", "D://hadoop-winutils-2.6.0//");
      val appConf = ConfigFactory.load()
      val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g");
      return  new SparkContext(conf);
    
  }
}