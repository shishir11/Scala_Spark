package practise.scala.spark.dao.connection

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.JdbcRDD
import java.sql.{Connection, DriverManager}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path;
import scala.collection.JavaConverters._
import java.util.List;
import java.util.ArrayList;

class SparkSqlClass {
  /*Publishing configuration object for hadoop*/
  def getConf(): SparkConf = {
    System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutls")
    /*Loading properties file*/
    val appConf = ConfigFactory.load();
    return new SparkConf().
      setAppName("Spark SQL APP").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true");
  }
  
  /*Instanctitaing SparkContext object*/
  def getSparkContext(): SparkContext = {
    /*Setting required properties*/
    val conf = getConf();
    /*creating sparkcontext through configured properties*/
    return new SparkContext(conf);
  }
  
  /*Instantitaing mysql connection object*/
  def establishMySqlConnection(): Connection ={
    val url = "jdbc:mysql://localhost:3306/sparkdb"
    val username = "root"
    val password = "root"

    Class.forName("com.mysql.jdbc.Driver").newInstance;
    return DriverManager.getConnection(url, username, password);
  }
  
  def showRecord() :List[String] = {
    val sc = getSparkContext();
    
    val fs = FileSystem.get(sc.hadoopConfiguration);
    val path = new Path("D:\\Softwares\\newperson");
    if (fs.exists(path)) {
      fs.delete(path);
      println("deleting the previous record...");
    }
    //val connection = establishMySqlConnection();
    val url = "jdbc:mysql://localhost:3306/sparkdb"
    val username = "root"
    val password = "root"

    Class.forName("com.mysql.jdbc.Driver").newInstance;
 
    val myRDD = new JdbcRDD(sc, () => DriverManager.getConnection(url, username, password),
      "select first_name,last_name,gender from person limit ?, ?",
      1, 5, 2, r => r.getString("last_name") + ", " + r.getString("first_name"));
    
    myRDD.saveAsTextFile("D:\\Softwares\\newperson");
    
    val  record = myRDD.collect().toList;
   
    return record.asJava;
  }
}