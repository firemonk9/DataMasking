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
    edf.show()

    val ndf = Mask.maskColumn(sampleFNew,"ssn","SSN",Mask.REAL_NUMBER,true,9)
    ndf.show()
    assert(edf.filter(edf("name") === "John").head() == ndf.filter(ndf("name") === "John").head())
    assert(edf.count() == sampleF.count())
    assert(ndf.count() == sampleFNew.count())

  }

  test("testMaskNumber multiple") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    import spark.implicits._

    val sample = spark.range(10000).toDF("ssn")
    val sampleF = sample.withColumn("name",sample("ssn"))

    val sample1 = spark.range(10000).toDF("ssn")
    val sampleFNew = sample1.withColumn("ssn",sample1("ssn")+5000)

    val edf = Mask.maskColumn(sampleF,"ssn","SSN",Mask.REAL_NUMBER,true,9)
    edf.show()
    val snTbl = spark.sql("select * from SSN").count()
    assert(snTbl == sampleF.count())

    val ndf = Mask.maskColumn(sampleFNew,"ssn","SSN",Mask.REAL_NUMBER,true,9)
    assert(sampleFNew.count() == ndf.count())
    val snTbl1 = spark.sql("select * from SSN").count()
    assert(snTbl1 == (sampleF.count()+5000))

    val ndf1 = Mask.maskColumn(sampleFNew,"ssn","SSN",Mask.REAL_NUMBER,true,9)
    assert(sampleFNew.count() == ndf1.count())

    ndf.show()


  }


}
