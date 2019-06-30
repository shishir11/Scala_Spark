package com.scala.spark.usecase

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._
import java.text.Collator

/**
 * Ref link: https://www.tutorialspoint.com/scala
 */
object EmployeeAnalysis {
  def getAvgByDept(list: List[Employee]): Map[String, Double] = {
    var map = Map[String, Double]()

    val groupMap = list.groupBy(emp => emp.department).toMap
    groupMap.keys.foreach(dept => {
      map += dept -> groupMap(dept).map(emp => emp.salary).reduce(_ + _) / groupMap(dept).size
    })

    return map;
  }

  def getAvgByDeisgnation(list: List[Employee]): Map[String, Double] = {
    var map = Map[String, Double]();

    val group = list.groupBy(emp => emp.designation).toMap
    group.keys.foreach(desg => {
      map += desg -> group(desg).map(emp => emp.salary).reduce(_ + _) / group(desg).size
    })

    return map;
  }

  def getSortedSalary(list: List[Employee]): List[Double] = {

    return list.map(emp => emp.salary).sorted.toList.reverse

  }

  def getMaxId(list: List[Employee]): Integer = {
    var set = scala.collection.mutable.Set[Int]()
    list.map(emp => emp.idNo).foreach(id => {
      set += id;
    })
    return set.max
  }

  def getMinId(list: List[Employee]): Integer = {
    var set = scala.collection.mutable.Set[Int]()
    list.map(emp => emp.idNo).foreach(id => {
      set += id;
    })
    return set.min
  }

  def main(args: Array[String]): Unit = {
    lazy val empDB = new EmpDB
    println("Average by department....")
    EmployeeAnalysis.getAvgByDept(empDB.populateList()).foreach(println)
    println("*****************************************************")

    println("Average by designationt....")
    EmployeeAnalysis.getAvgByDeisgnation(empDB.populateList()).foreach(println)
    println("*****************************************************")

    println("Max salary....")
    val salaryList = EmployeeAnalysis.getSortedSalary(empDB.populateList())
    println(salaryList.head);

    println("*****************************************************")

    println("Max id....")
    println(EmployeeAnalysis.getMaxId(empDB.populateList()))
    println("*****************************************************")
    
    println("Min id....")
    println(EmployeeAnalysis.getMinId(empDB.populateList()))
    println("*****************************************************")
  }
}