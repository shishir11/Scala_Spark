package practise.scala.spark.demo

import practise.scala.spark.dao.connection.SparkSqlClass;
import org.apache.spark.SparkContext
import java.sql.DriverManager
import java.sql.Connection
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

 
/**
 * https://community.hortonworks.com/questions/87441/error-only-one-sparkcontext-may-be-running-in-this.html
 * https://stackoverflow.com/questions/34879414/multiple-sparkcontext-detected-iarkan-the-same-jvm
 * https://data-flair.training/blogs/spark-sql-optimization/
 * https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/
 * https://data-flair.training/blogs/apache-spark-performance-tuning/
 * https://data-flair.training/blogs/apache-spark-performance-tuning/
 * https://data-flair.training/blogs/fault-tolerance-in-apache-spark/
 * https://spark.apache.org/docs/2.2.1/running-on-mesos.html
 * https://stackoverflow.com/questions/44594134/value-tods-is-not-a-member-of-org-apache-spark-rdd-rdd
 */
case class Employee(name: String, age: Int, departmentId: Int, salary: Double)
case class Department(id: Int, name: String)
case class Company(name: String, foundingYear: Int, numEmployees: Int)
case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)
case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

object DatasetDemo {

  def initiateSparkContext(): SparkContext = {
    val sparkConnection = new practise.scala.spark.connection.Connection();
    return sparkConnection.getSparkContext();
  }
  def performJoinOnDataset(): Unit = {
    val sc = initiateSparkContext();
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val employeeDataSet1 = sc.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
    val employeeDataSet2 = sc.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
    val departmentDataSet = sc.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()

    println("Joining two different dataset....");
    val employeeDataset = employeeDataSet1.union(employeeDataSet2);

    println("performing dataframe and rdd operation on joined dataset....and extracting the record.");
    val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
      .map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name))
      .filter(record => record.age > 25)
      .groupBy($"departmentId", $"departmentName")
      .avg();

    println("displaying the average salary.....");
    averageSalaryDataset.show()
  }

  def performCommonDatasetOperation(): Unit = {
    val sparkContext = initiateSparkContext();
    val sqlContext = new SQLContext(sparkContext);
    val sparkSession = sqlContext.sparkSession;

    import sqlContext.implicits._
    println("Creating for dataset... from the seq of array");
    val employeeDataSet1 = sparkContext.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
    val employeeDataSet2 = sparkContext.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
    val departmentDataSet = sparkContext.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()

    println("To display the record...");
    employeeDataSet1.show();

    println("Creating of dataset from the dataframe...");
    val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83));
    val df = sparkContext.parallelize(inputSeq).toDF();
    
    val inputSeq1 = Seq(Company("Tavant Tech", 1998, 310), Company("ITC", 1983, 904), Company("GlobalLogic", 2005, 83));
    val df1 = sparkContext.parallelize(inputSeq1).toDF();
    
    println("Providing a type safety including a schema...");
    val companyDS = df.as[Company];
    companyDS.show();
    
    val nextCompanyDS = df1.as[Company];
    println("Displaying the record using analyzed attribute");
    companyDS.union(nextCompanyDS).queryExecution.analyzed.foreach(println);
    
    println("filtering null value providing a condition as a expression....");
    companyDS.filter("foundingYear is not null").as[Company].show();
  }
  def main(arr: Array[String]): Unit = {

   // performJoinOnDataset();
    performCommonDatasetOperation;
  }
}