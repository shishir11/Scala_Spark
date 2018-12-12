package practise.scala.spark.aws.emr

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object EMRDemo {
  def main(arr: Array[String]) = {
    //System.setProperty("hadoop.home.dir", "D://hadoop-winutils-2.6.0//");
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode"));

    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    val sparkSession = sqlContext.sparkSession;
    val input = arr(0);
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(input);

 //   println("This wouldn't work with normal csv method val df = sqlContext.read.csv(D://Project//scala//ratings//ratings.csv).toDF()");

    println("Printing Schema.");
    df.printSchema();

    println("Display limited record.....[userId,movieId,rating,timestamp");
    val out = df.select("userId", "movieId", "rating").take(100).toArray;
    val rdd = sc.parallelize(out);
    rdd.saveAsTextFile("s3://aws-logs-777592766282-ap-south-1/elasticmapreduce/out.txt")
    
    
  }
}