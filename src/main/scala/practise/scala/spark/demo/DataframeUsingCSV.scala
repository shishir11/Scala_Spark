package practise.scala.spark.demo

import org.apache.spark.sql.SQLContext
/**
 * Ref link: https://stackoverflow.com/questions/30332619/how-to-sort-by-column-in-descending-order-in-spark-sql
 * https://mvnrepository.com/artifact/com.databricks/spark-csv_2.11/1.1.0
 * https://docs.databricks.com/spark/latest/dataframes-datasets/introduction-to-dataframes-scala.html
 */
object DataframeUsingCSV {
  def performDataFrameOperation(): Unit = {
    val sc = new practise.scala.spark.connection.Connection();
    val sqlContext = new SQLContext(sc.getSparkContext());
    val sparkSession = sqlContext.sparkSession;

    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("D://Project//scala//ratings//ratings.csv");

    println("This wouldn't work with normal csv method val df = sqlContext.read.csv(D://Project//scala//ratings//ratings.csv).toDF()");

    println("Printing Schema.");
    df.printSchema();

    println("Display limited record.....[userId,movieId,rating,timestamp");
    df.select("userId", "movieId", "rating").take(20).toList.foreach(println);

    println("Performing order by query on dataframe on ascending order...");
    df.select("userId", "movieId", "rating").orderBy("movieId").take(50).seq.foreach(println);

    println("Performing order by query on dataframe on descending order...");
    import org.apache.spark.sql.functions._
    df.select("userId", "movieId", "rating").sort(desc("userId")).take(10).seq.foreach(println);

    println("Performing filter operation....when rating is greater than 3");
    df.select("userId", "movieId", "rating").filter(df("rating") > 3).take(10).toList.foreach(println);

    println("Performing filter operation....when rating is 3 and movie Id is greate than 30");
    df.select("userId", "movieId", "rating").filter(df("rating") === 3 && df("movieId") >= 30).take(10).toList.foreach(println);

    println("Performing groupby operation....");
    df.select("userId", "movieId", "rating").filter(df("movieId") <= 30).groupBy("rating").agg(countDistinct("rating") as "rating accroding to the group").take(10).toList.foreach(println);

    println("Performing groupby operation....");
    df.select("userId", "movieId", "rating").filter(df("movieId") <= 30).groupBy("rating").agg(countDistinct("rating") as "rating accroding to the group").take(10).seq.foreach(println);

    val joindf1 = df.select("userId", "movieId", "rating").filter(df("movieId") <= 300).groupBy("rating").agg(countDistinct("rating") as "rating accroding to the group").toDF();
    val joindf2 = df.select("userId", "movieId", "rating").filter(df("movieId") >= 300).groupBy("rating").agg(countDistinct("rating") as "rating accroding to the group").toDF();

  }
  def main(arr: Array[String]): Unit = {
      performDataFrameOperation;
  }
}