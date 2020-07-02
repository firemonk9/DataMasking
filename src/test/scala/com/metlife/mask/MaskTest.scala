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
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val spark = sqlContext.sparkSession
    import spark.implicits._
    import org.apache.spark.sql._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.{lit, max, row_number}


    val df = spark.range(5).toDF("ssn").withColumn("abc",lit("12"))
    val sampleF = df.union(df).union(df).union(df).union(df).union(df).union(df).union(df).union(df).union(df)

    val edf = Mask.maskColumn(sampleF,"ssn","SSN",Mask.REAL_NUMBER,true,9)
    assert(sampleF.count() == edf.count())

    val edf1 = Mask.maskColumn(sampleF,"ssn","SSN",Mask.REAL_NUMBER,true,9)
    assert(sampleF.count() == edf.count())




//
    //
    //    val edf = Mask.maskColumn(sampleF,"ssn","SSN",Mask.REAL_NUMBER,true,9)
//    edf.show()
//    val snTbl = spark.sql("select * from SSN").count()
//    assert(snTbl == sampleF.count())
//
//    val ndf = Mask.maskColumn(sampleFNew,"ssn","SSN",Mask.REAL_NUMBER,true,9)
//    assert(sampleFNew.count() == ndf.count())
//    val snTbl1 = spark.sql("select * from SSN").count()
//    assert(snTbl1 == (sampleF.count()+5000))
//
//    val ndf1 = Mask.maskColumn(sampleFNew,"ssn","SSN",Mask.REAL_NUMBER,true,9)
//    assert(sampleFNew.count() == ndf1.count())
//
//    ndf.show()


  }


}
