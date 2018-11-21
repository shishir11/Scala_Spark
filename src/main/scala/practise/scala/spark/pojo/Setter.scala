package practise.scala.spark.pojo

import practise.scala.spark.pojo.Employee;

class Setter {
  def getEmployeeData(): Unit = {
    val employee1 = Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = Employee("matei", null, "no-reply@waterloo.edu", 140000)
    val employee4 = Employee(null, "wendell", "no-reply@princeton.edu", 160000)
    val seqOfEmp = Seq(employee1, employee2, employee3, employee4)
    // return seq;
  }

  def getDepartmentData(): Unit = {
    // Create the Departments
    val department1 = new Department("123456", "Computer Science")
    val department2 = new Department("789012", "Mechanical Engineering")
    val department3 = new Department("345678", "Theater and Drama")
    val department4 = new Department("901234", "Indoor Recreation")
  }
}