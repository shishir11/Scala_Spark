package com.scala.spark.usecase

import com.scala.spark.usecase._
case class Employee (name : String, department : String, salary : Double,city : String ,
    state : String, country : String , idNo : Int, phone : String, designation : String
    , machine : String)
    
class EmpDB {
  def populateList(): List[Employee]={
    val list = List[Employee](new Employee("Peter","IT",1000,"Bangalore","Karnataka","India",1,"1134456790","SE","Desktop"),
                    new Employee("Bruce","IT",1400,"Bangalore","Karnataka","India",2,"1133456790","SE","Desktop"),
                    new Employee("Pat","IT",2000,"Pune","Maharastra","India",3,"1232456790","SE","Desktop"),
                    new Employee("Pepper","HR",3000,"Bangalore","Karnataka","India",4,"1236456790","HRE","Desktop"),
                    new Employee("John","IT",5000,"Pune","Maharastra","India",5,null,"SSE","Desktop"),
                    new Employee("Jack","IT",4000,"Chennai","TamilNadu","India",6,"9178456790","SSE","Desktop"),
                    new Employee("Steve","IT",8000,"Mumbai","Maharastra","India",7,"1244456790","SSE","Desktop"),
                    new Employee("Jim","IT",71700,"Pune","Maharastra","India",8,null,"Lead","Laptop"),
                    new Employee("Clarke","IT",66000,"Chennai","TamilNadu","India",9,"1234356790","Lead","Laptop"),
                    new Employee("Tony","IT",75200,"Bangalore","Karnataka","India",10,"913477790","Lead","Laptop"),
                    new Employee("Pepper2","HR",8000,"Pune","Maharastra","India",11,"9136456790","HRE","Desktop"),
                    new Employee("AvengerS","HR",8000,"Chennai","TamilNadu","India",12,"1236456790","HRE","Desktop"),
                    new Employee("Avik abcsS","Finance",1100,"Pune","Maharastra","India",13,"1236456790","F","Desktop"),
                    new Employee("Finance2","Finance",1200,"Bangalore","Karnataka","India",14,"1236456790","F","Desktop"),
                    new Employee("Finance3","Finance",1500,"Chennai","TamilNadu","India",15,"1236456790","F","Desktop")   
            )
    list
  }
}