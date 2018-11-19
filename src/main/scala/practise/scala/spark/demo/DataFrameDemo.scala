package practise.scala.spark.demo
import practise.scala.spark.dao.connection.SparkSqlClass;
import org.apache.spark.SparkContext
import java.sql.DriverManager
import java.sql.Connection
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object DataFrameDemo {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "D://hadoop-winutils-2.6.0//");
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g");
    val sc = new SparkContext(conf);

    val sqlContext = new SQLContext(sc);
    val sparkSession = sqlContext.sparkSession;

    val df = sparkSession.read.json("src/main/resources/people.json");
    //Printing schema
    df.printSchema();

    //select specific column
    df.select("name").show();

    // Select everybody, but increment the age by 1
   // df.select("name", $"age" + 1).show();

    df.distinct().show();

    // Select people older than 21
    //df.filter($"age" > 21).show()

    // Count people by age
   // df.groupBy("age").count().show();

    // Register the DataFrame as a SQL temporary view
    //The sql function on a SparkSession enables applications to run SQL queries programmatically and returns the result as a DataFrame.
    df.createOrReplaceTempView("people")
    println("Register the DataFrame as a SQL temporary view");
    val sqlDF = sparkSession.sql("SELECT * FROM people")
    sqlDF.show();

    //Global Temporary View
    /* Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.
    If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application
    terminates, you can create a global temporary view. Global temporary view is tied to a system preserved database
    global_temp, and we must use the qualified name to refer it, e.g. SELECT * FROM global_temp.view1.*/
    df.createGlobalTempView("people")
     println("Register the DataFrame as a SQL Global Temporary View");
    // Global temporary view is tied to a system preserved database `global_temp`
    sparkSession.sql("SELECT * FROM global_temp.people").show();
  }
}