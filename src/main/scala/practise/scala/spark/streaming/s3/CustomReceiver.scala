/*package practise.scala.spark.streaming.s3

import scala.util.Random
import org.apache.spark.streaming.receiver._

class CustomReceiver (ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  def generateData(): String = {
    (1 to 100).map(i => "Random" + i + Random.nextInt(100000)).mkString(",")
  }
  
  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
   // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself isStopped() returns false
  }

  *//** Create a socket connection and receive data until receiver is stopped *//*
  private def receive() {
    while(!isStopped()) {      
      store(generateData())
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}*/