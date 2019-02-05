package com.metlife.mask

//case class MaskType(maskNumber:Option[String],)
// mask_db
//
import org.apache.spark.sql.DataFrame

object Mask {

  val maskDatabase = "MASK_DB"
  val REAL_NUMBER = "REAL_NUMBER"
  def main(args: Array[String]): Unit = {
    //create spark context
    //read properties
  }

  def maskColumn(df:DataFrame, columnName:String,globalName:String ,maskType:String, createIfNotExists:Boolean=false,numberOfDigits:Int= -1):DataFrame={
    if(maskType == REAL_NUMBER)
      maskNumber(df,columnName,globalName,createIfNotExists,numberOfDigits)
    else
      df
  }

  def maskNumber(df1:DataFrame, columnName:String,globalName:String, createIfNotExists:Boolean=false,numberOfDigits:Int):DataFrame={
    import org.apache.spark.sql.functions.{min, max}
    import org.apache.spark.sql.Row

    val df = df1.select(columnName)
//    df.sparkSession.sql("use "+maskDatabase)

    val  (begin:Int, toAppend:Option[DataFrame], newDF:DataFrame) = if(!df.sparkSession.catalog.tableExists(globalName) && createIfNotExists==false){
      throw new Exception("Check the name "+globalName+" or set createIfNotExists to true if new table should be created.")
    }
    else if(!df.sparkSession.catalog.tableExists(globalName) && createIfNotExists){
      val begin = if(numberOfDigits == 8) 10000000
      else if(numberOfDigits == 9) 100000000
      else if(numberOfDigits == 10) 1000000000
      else 1000 //TODO change
      (begin, None, df)
    }
    else if(df.sparkSession.catalog.tableExists(globalName)){
      val Row(maxValue: Double) = df.agg(max(df("value"))).head
      val begin = (maxValue + 1).toInt
      val existingDF = df.sparkSession.sql("select key,value from "+globalName)
      val jdf = df.join(existingDF,df(columnName)===existingDF("key")).drop("key").withColumnRenamed("value",columnName)
      val toAddDf = df.except(jdf.select(columnName))
      (begin, Some(jdf), toAddDf)
    }


    df
  }
}
