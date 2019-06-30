package practise.spark.basic

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * https://data-flair.training/blogs/spark-rdd-operations-transformations-actions/
 */
object ScalaDemo {

  def initiateSparkContext(): SparkContext = {
    val sparkConnection = new practise.scala.spark.dao.connection.Connection();
    return sparkConnection.getSparkContext();
  }

  def main(args: Array[String]) {

    val sc = initiateSparkContext;
    println("Spark context loading succesfully: " + sc.getClass.toString());
    //  sc.stop();
    val data = Array(1, 2, 3, 4, 5);
    // foreach demo
    val distData = sc.parallelize(data, 1);
    
    distData.foreach(println);
    distData.map(println);
    distData.collect().foreach(println);
    distData.take(4).foreach(println);

    /*Sort the data */ 
    distData.map(_.toInt).sortBy(t => t, true)

    /*Number change into string without zero*/
    val appendedData = distData.filter(fil => fil > 0).map(num => num.toString()).reduce(_ + _);
    println(appendedData);

    /*Calculate the average if Integer array*/

    val avgOfDistData = distData.filter(fil => fil > 0).reduce(_ + _) / (distData.count());

    /*Appended the +91 in all the indian no and also filter where should not be zero and length be 10*/
    val phoneNo = List("9981725015", "9691633929", "8779144752");
    phoneNo.filter(num => num.length() > 10).map(num => num.toInt).filter(fil => fil.!=(0)).map(newnum => newnum.toString()).map(num => "+91" + num).take(3);

    //phoneNo.isInstanceOf[List].!=(true)
    
    /*FlatMap example */
    val flatMapData = sc.parallelize(List("spark rdd example",  "sample example"), 2)
    
    flatMapData.flatMap( data => data.split(" ")).collect().foreach(println);
    
    phoneNo.flatMap(num => num.concat("000"))
    
    /*Sort by second element of the list*/
    phoneNo.sorted.foreach(println)
    println(phoneNo.sortBy(ph => ph))

    List(('b', 30), ('c', 10), ('a', 20)).sortBy(_._2).foreach(println);
    List(('b', 30), ('c', 10), ('a', 20)).sortBy(_._1).foreach(println);
    
    
    /*Code for the BroadCast variable*/
    val broadcastVar = sc.broadcast(Array(1, 2, 3));
    broadcastVar.value.foreach(println);
    
    
    /*Accumulator*/
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