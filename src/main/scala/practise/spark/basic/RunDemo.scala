package practise.spark.basic

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory

object RunDemo {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Temp\bin\\winutls")
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g");
    val sc = new SparkContext(conf);
    println("Spark context loading succesfully: " + sc.getClass.toString());
    //  sc.stop();

    val textReader = sc.textFile("C:\\Shishir\\filedemo.txt");

    /*It will create Spark RDD Collection*/
    var startTIme = System.currentTimeMillis();
    val rdd = sc.parallelize("C:\\Shishir\\filedemo.txt");
    rdd.foreach(println);
    var endTIme = System.currentTimeMillis();

    println("Total execution time for RDD: " + (endTIme - startTIme));

    /*It will create List of String*/
    startTIme = System.currentTimeMillis();
    val result = textReader.collect();
    result.foreach(println);
    endTIme = System.currentTimeMillis();
    println("Total execution time for List: " + (endTIme - startTIme));

    val counts = textReader.flatMap(line => line.split(",")).map(word => (word, 1))
      .reduceByKey(_ + _)
    println("No of count is: " + counts.count());

    val word=textReader.map(line => line.split(",").size)
    .reduce((a, b) => 
      if (a > b) a else b)
     println("No of word is: " + word);
    sc.stop();
  }
}