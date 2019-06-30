package practise.scala.spark.dfds

import org.apache.spark.sql.SQLContext

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructField, StructType }
import org.apache.spark.sql.SQLContext

object DataFrameDemo {

  def empJSONRecord(sparkSession: SparkSession): Unit = {

    import sparkSession.implicits._
    val df = sparkSession.read.json("src/main/resources/people_old.json");
    //Printing schema
    df.printSchema();

    println("select specific column");
    df.select("name", "email", "creditcard", "city", "mac").show();

    println(" Select everybody, but increment the age by 1");

    import org.apache.spark.sql.functions._

    df.select("name", "age", "email", "creditcard", "city", "mac").filter(df("name").isNotNull);
    df.select(df("name"), df("age") + 1).show();

    df.select($"age" + 2).show();
    df.distinct().show();

    println(" Select people older than 21");
    df.filter(df("age") > 21).show();

    println(" Count people by age");
    df.groupBy("age").count().show();

    println("Displaying average name and age.......");
    df.select("name", "email").agg(avg($"creditcard")).as("creditcard").show();

    // Register the DataFrame as a SQL temporary view
    println("The sql function on a SparkSession enables applications to run SQL queries programmatically and returns the result as a DataFrame.");
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

  def empCaseClassRecord(spark: SparkContext): Unit = {

    case class Department(id: String, name: String)
    case class Employee(firstName: String, lastName: String, email: String, salary: Int)
    case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])

    // Create the case classes for our domain

    // Create the Departments
    val department1 = new Department("123456", "Computer Science")
    val department2 = new Department("789012", "Mechanical Engineering")
    val department3 = new Department("345678", "Theater and Drama")
    val department4 = new Department("901234", "Indoor Recreation")

    // Create the Employees
    val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
    val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)

    // Create the DepartmentWithEmployees instances from Departments and Employees
    val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
    val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
    val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee1, employee4))
    val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))
    
   // import sqlContext.implicits._
    
    val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
  //  val df1 = departmentsWithEmployeesSeq1.toDF()

    val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
  //  val df2 = departmentsWithEmployeesSeq2.toDF()

  }

  def main(args: Array[String]) {
    val sc = new practise.scala.spark.dao.connection.Connection();
    val spark = sc.getSparkContext();
    val sqlContext = new SQLContext(spark);
    val sparkSession = sqlContext.sparkSession;
  }

}