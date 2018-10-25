package practise.scala.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory

object SparkPractise {
  def main(args: Array[String]) {
    
     val t = new Tuple3(1, "hello", Console)
    t.productIterator.foreach{ i =>println("Value = " + i )}
     
    System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutls")
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g");
    val sc = new SparkContext(conf);
    println("Spark context loading succesfully: " + sc.getClass.toString());

    val lines = sc.textFile("D:\\Data\\filedemo.txt");

    val lengthCounts = lines.map(line => (line.length, 1)).reduceByKey(_ + _)
    
    println("Data print get started...: ");
    lines.collect().foreach(println);

    /*val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    val accum = sc.accumulator(0);
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x);
*/
    /*val pairs = lines.map(s => (s, 1));
    val counts = pairs.reduceByKey((a, b) => a + b);
    //counts.reduceByKey(func)
   println("Data print get started...: ");
   counts.collect().foreach(println);
   println("Data print of top 100 get started...: ");
   counts.top(100).foreach(println);
   println("Data print of top 10 get started...: ");
   counts.take(10);*/
  }
}