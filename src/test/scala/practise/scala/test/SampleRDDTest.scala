package practise.scala.test


import org.scalatest.FunSuite


class SampleRDDTest extends FunSuite  {
  test("really simple transformation") {
    val input = List("hi", "hi cloudera", "bye")
    val expected = List(List("hi"), List("hi", "cloudera"), List("bye"))
   // expected.map(_ + "").collect(Collator.);
    //expected.flatMap( _ + "").collect();
    val transforExpected = expected.flatten;
    assert(input.take(0).equals(transforExpected.take(0)));
    //assert(SampleRDD.tokenize(sc.parallelize(input)).collect().toList === expected)
  }
}