package practise.scala.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory


object SparkPractise {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutls")
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g");
    val sc = new SparkContext(conf);
    println("Spark context loading succesfully: " + sc.getClass.toString());

    val lines = sc.textFile("D:\\Data\\filedemo.txt");
    val pairs = lines.map(s => (s, 1));
    val counts = pairs.reduceByKey((a, b) => a + b);
    //counts.reduceByKey(func)
   println("Data is available: "+counts.collect());
  }
}