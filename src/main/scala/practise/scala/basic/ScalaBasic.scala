package practise.scala.basic

case class Employee(name: String, idProof: String);

object ScalaBasic {

  def main(args: Array[String]): Unit = {

    //Immutable variables
    //val <Name of our variable>: <Scala type> = <Some literal>

    val record: Int = 5;
    println(record);
    val nextRecord = 10;
    println(record);

    //Mutable variables
    var textRecord: String = "text data";
    textRecord = "another text data";
    println(textRecord)

    //Lazy initialization
    lazy val lazyInitializeRecord = 100000000;
    println(lazyInitializeRecord);

    //Supported Type:
    val longTypeRecord: Long = 100000000L
    val sortTypeRecord: Short = 1
    val doubleTypeRecord: Double = 2.50
    val floatTypeRecord: Float = 2.50f
    val stringTypeRecord: String = "text data"
    val byteTypeRecord: Byte = 0xa
    val charTypeRecord: Char = 'D'
    val voidTypeRecord: Unit = ()

    //Conversion
    val convertRecord: Short = 1;
    val intRecord: Int = convertRecord;
    val textNextRecord: String = convertRecord.toString();

    //String interpolation to print a variable
    println(s"Lazy initialize value is ${lazyInitializeRecord}")

    println("\nStep 4: Using if and else clause as expression")
    val checkRecord = if (intRecord > 10) (intRecord * convertRecord) else intRecord
    println(s"Record is = $checkRecord")

    //Loop statement.
    println("Step 1: A simple for loop from 1 to 5 inclusive")
    for (index <- 1 to 5) {
      println(s"Index position: = $index")
    }

    println("Step 1: A simple for loop from 1 to 5 inclusive")
    for (index <- 1 until 4) {
      println(s"Index position: = $index")
    }

    val technology = List("java", "scala", "spark", "hive", "oozie")
    for (techno <- technology if techno == "hive") {
      println(s"Found technologies = $techno")
    }
    // while and do while has same syntax as in java
    //2-Dimensional array.
    val twoDimensionalArray = Array.ofDim[String](2, 2)
    twoDimensionalArray(0)(0) = "java"
    twoDimensionalArray(0)(1) = "scala"
    twoDimensionalArray(1)(0) = "spark"
    twoDimensionalArray(1)(1) = "hive"
    for {
      x <- 0 until 2
      y <- 0 until 2
    } println(s"${(x, y)} = ${twoDimensionalArray(x)(y)}")

    // Pattern matching..
    println("Step 1: Pattern matching ")
    var technologyType = "Java"
    technologyType match {
      case "Java"  => println("Running jdk 8 version")
      case "Scala" => println("Running scala 2.12 version")
    }

    technologyType = "N/A"
    val techis = technologyType match {
      case "Java"  => "Running jdk 8 version";
      case "Scala" => "Running scala 2.12 version";
      case _       => "Need to check";
    }
    println(s"Technology type is: $techis")

    println("\nStep 6: Pattern matching by types")
    val prices: Any = 2.50f
    val priceType = prices match {
      case price: Int     => "Int"
      case price: Double  => "Double"
      case price: Float   => "Float"
      case price: String  => "String"
      case price: Boolean => "Boolean"
      case price: Char    => "Char"
      case price: Long    => "Long"
    }
    println(s" price type = $priceType")

    // hold objects with different types but they are also immutable.
    val technologyStack = Tuple2("Java", "Scala")
    println(s"$technologyStack._1 ..... $technologyStack._2")

    val technologyStackWithVer = Tuple3("Java", "Scala", 1.8)
    println(s"$technologyStack._1 ..... $technologyStack._2..... $technologyStack._3")

    val technologyStackWithVer1 = Tuple4("Java", "Scala", 1.8, 2.12)
    val list = List(technologyStackWithVer1, technologyStackWithVer, technologyStack)
    //list.foreach(println);
    list.foreach { tuple =>
      {
        tuple match {
          case ("Java", "Scala") => println(s"Java and Scala version= ${1.8} and {2.12}")
          case _                 => None
        }
      }
    }
    //N no of group..
    val technologyStackWithVer2 = ("Java", "Scala", 1.8, 2, 45, 65)

    //Case class example
    val emp: Employee = Employee("Shishir", "Driving Licence");
    println(s"Employee details is ${emp.name} , ${emp.idProof}")

    val nextEmp = Employee("Shishir", "Pan Card");
    println(s"Employee details is ${nextEmp.name} , ${nextEmp.idProof}")

    //Optional API using Option and Some
    val bigData: Option[String] = Some("Big Data")
    println(s"value = ${bigData.get}")

    val noneValue: Option[String] = None;
    println(s"defaul value is: ${noneValue.getOrElse("AWS")}")

    bigData.map(tech => println(s"${tech}"))
    bigData.map(tech => tech.contains("g D")).foreach(println);

    //clouser function in scala
    var votingAge = 18
    val isOfVotingAge = (age: Int) => age >= votingAge
    printResult(isOfVotingAge, 10);
    
  }

  def printResult(f: Int => Boolean, x: Int) {
    println(f(x))
  }
}