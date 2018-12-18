package com.scala.spark.usecase



object EmployeeAnalysis {
  def getAvgByList(list: List[Employee]):Map[String, Double]={
    var map = Map[String, Double]()
    
    val groupMap = list.groupBy( emp => emp.department).toMap
    groupMap.keys.foreach(dept => {
      map += dept -> groupMap(dept).map(emp => emp.salary).reduce(_ + _) / groupMap(dept).size
    })
    
    return map;
  }
  def main(args: Array[String]): Unit = {
    lazy val empDB = new EmpDB
    EmployeeAnalysis.getAvgByList(empDB.populateList()).foreach(println)
  }
}