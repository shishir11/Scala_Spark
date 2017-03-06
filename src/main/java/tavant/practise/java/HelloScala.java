package tavant.practise.java;

import java.util.ArrayList;
import java.util.List;

import practise.scala.spark.dao.connection.SparkSqlClass;


public class HelloScala {
	
	ArrayList<String> arrayList = new ArrayList<String>();
	public  List<String> runSparkJob() {
		SparkSqlClass scala = new SparkSqlClass();
		List<String> record = scala.showRecord();
		return record;
	}
}
