package practise.scala.spark.dao.crud

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.JdbcRDD
import java.sql.{ Connection, DriverManager }
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SQLContext;

object ScalaDataFrame {
  def main(shishir: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\Softwares\\winutls")
    /*Loading properties file*/
    val appConf = ConfigFactory.load();
    val sparkConf = new SparkConf().
      setAppName("Spark SQL APP").
      setMaster(appConf.getConfig("dev").getString("executionmode")).
      set("spark.executor.memory", "1g");
    val sparkContext = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sparkContext);
    val url = "jdbc:mysql://localhost:3306/sparkdb?user=root;password=root"
    val df = sqlContext.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "person")
      .load()

   /* // Looks the schema of this DataFrame.
    df.printSchema()

    // Counts people by age
    val countsByAge = df.groupBy("age").count()
    countsByAge.show()*/

    // Saves countsByAge to S3 in the JSON format.
    //countsByAge.write.format("json").save("s3a://...")
  }
}