package practise.scala.spark.dao.connection

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

class Connection {
  def getSparkContext(): SparkContext = {
      System.setProperty("hadoop.home.dir", "C://Temp//bin//");
      val appConf = ConfigFactory.load()
      val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts","true");
      return  new SparkContext(conf);
    
  }
}