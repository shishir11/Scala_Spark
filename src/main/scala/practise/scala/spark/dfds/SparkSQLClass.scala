package practise.scala.spark.dfds

import org.apache.spark.sql.SQLContext
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.DriverManager
import java.sql.Connection

object SparkSQLClass {
  def getDBCOnnection(url: String, username: String, password: String): Connection = {
    System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutls")
    val appConf = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("Word Count").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g");
    val sc = new SparkContext(conf);

    val sqlContext = new SQLContext(sc);
    val sparkSession = sqlContext.sparkSession;

    Class.forName("com.mysql.jdbc.Driver").newInstance;
    return DriverManager.getConnection(url, username, password);

  }
  def main(args: Array[String]) {
    val url = "jdbc:mysql://localhost:3306/mysql"
    val username = "root"
    val password = "root"
  }
}