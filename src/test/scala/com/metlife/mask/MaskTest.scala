package com.metlife.mask

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite
import org.apache.spark.sql._


case class Person(name:String, ssn:Int, addr:String)

class MaskTest extends FunSuite with SharedSparkContext {

  val sampleData = List(Person("John",123456,"abc Cary, nc"),Person("Rob",123457,"abc Cary, nc"),Person("Mary",123458,"abc Cary, nc"))
  val sampleDataNew = List(Person("John",123456,"abc Cary, nc"),Person("Harry",123434,"abc Cary, nc"))

  test("testMaskDatabase") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession

    assert(1==1)
  }

  test("testMaskNumber") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    val sampleF = spark.sparkContext.parallelize(sampleData).toDF
    val sampleFNew = spark.sparkContext.parallelize(sampleDataNew).toDF
    val edf = Mask.maskColumn(sampleF,"ssn","SSN",Mask.REAL_NUMBER,true,9)
    val ndf = Mask.maskColumn(sampleFNew,"ssn","SSN",Mask.REAL_NUMBER,true,9)
    assert(edf.filter(edf("ssn") === "John").head() == ndf.filter(edf("ssn") === "John").head())

  }

}
