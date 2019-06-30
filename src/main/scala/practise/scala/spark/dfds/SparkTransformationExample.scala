package practise.scala.spark.dfds

object SparkTransformationExample {
  def main(args: Array[String]): Unit = {
    val sparkConnection = new practise.scala.spark.dao.connection.Connection();
    val sparkContext = sparkConnection.getSparkContext();
    
    println("Flatmap transformation result....");
    sparkContext.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x)).collect.foreach(print);
    
    
    
    println("Map transformation result....");
    sparkContext.parallelize(List(1,2,3)).map(x=>List(x,x,x)).collect.foreach(print);
    
    

    
  }
}