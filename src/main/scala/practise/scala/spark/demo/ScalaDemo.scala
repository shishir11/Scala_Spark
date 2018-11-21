package practise.scala.spark.demo

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ScalaDemo {

 def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutls")
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g");
    val sc = new SparkContext(conf);
    println("Spark context loading succesfully: " + sc.getClass.toString());
    //  sc.stop();
    val data = Array(1, 2, 3, 4, 5);
    // foreach demo
    val distData = sc.parallelize(data, 1);
    distData.foreach(println);
    distData.map(println);
    distData.collect().foreach(println);
    distData.take(4).foreach(println);

    // BroadCast
    val broadcastVar = sc.broadcast(Array(1, 2, 3));
    broadcastVar.value.foreach(println);
    //Accumulator
    val accum = sc.accumulator(0)
    println("Accumulation Started......");
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x);
    println(accum.value);
    // Perform map and reduce
    val lines = sc.textFile("src/main/resources/application.conf");
    /* val Lenght = lines.map(s => s.length);
    val totalLength = Lenght.reduce((a, b) => a + b);
    println(totalLength);*/

    val counts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.collect();

    println(counts);
  }
}