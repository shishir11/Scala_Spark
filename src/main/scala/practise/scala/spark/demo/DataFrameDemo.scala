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
    
    val sc = new practise.scala.spark.connection.Connection();
    val sqlContext = new SQLContext(sc.getSparkContext());
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

    // Create the case classes for our domain
    case class Department(id: String, name: String)
    case class Employee(firstName: String, lastName: String, email: String, salary: Int)
    case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])

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

    val employeList: List[Employee] = List(employee1, employee2, employee3, employee4);
    
  //  sc.parallelize(employeList)
    //  sparkSession.createDataFrame(employeList,Employee);

  }

}