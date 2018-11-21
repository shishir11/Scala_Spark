package practise.scala.spark.demo;

import java.util.List;

import practise.scala.spark.dao.connection.SparkSqlClass;

public class JavaScalaDemo {
	public List<String> runSparkJob() {
		return new SparkSqlClass().showRecord();
	}

	public static void main(String[] args) {
		new JavaScalaDemo().runSparkJob().parallelStream().forEach(System.out::println);
	}
}
