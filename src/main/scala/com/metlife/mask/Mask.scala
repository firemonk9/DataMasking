package com.metlife.mask

//case class MaskType(maskNumber:Option[String],)
// mask_db
//
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.DataFrame
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.hive.ql.exec.UDF


object Mask {

  val maskDatabase = "MASK_DB"
  val REAL_NUMBER = "REAL_NUMBER"
  val maskKeyColumn = "mask_key_secret"
  val maskValueColumn = "mask_value"

  def main(args: Array[String]): Unit = {
    //create spark context
    //read properties
  }

  def maskColumn(df: DataFrame, columnName: String, globalName: String, maskType: String, createIfNotExists: Boolean = false, numberOfDigits: Int = -1): DataFrame = {
    if (maskType == REAL_NUMBER)
      maskNumber(df, columnName, globalName, createIfNotExists, numberOfDigits)
    else
      df
  }

  def maskNumber(dfToMask: DataFrame, columnName: String, globalName: String, createIfNotExists: Boolean = false, numberOfDigits: Int): DataFrame = {
    import org.apache.spark.sql.functions.{min, max}
    import org.apache.spark.sql.Row

    //    val dfToMask = dfToMask1.select(columnName)


    val (begin: Int, toAppend: Option[DataFrame], newDF: DataFrame) = if (!dfToMask.sparkSession.catalog.tableExists(globalName) && createIfNotExists == false) {
      throw new Exception("Check the name " + globalName + " or set createIfNotExists to true if new table should be created.")
    }
    else if (!dfToMask.sparkSession.catalog.tableExists(globalName) && createIfNotExists) {
      val begin = ("1" + "0" * numberOfDigits).toInt
      (begin, None, dfToMask)
    }
    else if (dfToMask.sparkSession.catalog.tableExists(globalName)) {
      val Row(maxValue: Double) = dfToMask.agg(max(dfToMask(maskValueColumn))).head
      val begin = (maxValue + 1).toInt

      val lookupDF = dfToMask.sparkSession.sql("select " + maskKeyColumn + "," + maskKeyColumn + " from " + globalName)
      val dfToMaskEnc = getEncryptedColumn(dfToMask,columnName)

      val jdf = dfToMaskEnc.join(lookupDF, dfToMaskEnc(columnName) === lookupDF(maskKeyColumn)).drop(maskKeyColumn).withColumnRenamed(maskValueColumn, columnName)
      val toAddDf = dfToMaskEnc.except(jdf.select(columnName))
      (begin, Some(jdf), toAddDf)
    }

    if (toAppend.isDefined) {

    }

    dfToMask
  }


  /**
    * This function encrypts the column and makes SHA-2 of the encrypted column.
    *
    * @param df
    * @param colName
    * @return
    */
  def getEncryptedColumn(df: DataFrame, colName: String): DataFrame = {
    val spark = df.sparkSession
    import org.apache.spark.sql.functions._
    spark.udf.register("encryptInt", encryptInt _)
    spark.udf.register("encryptStr", encryptStr _)

    val encInt = udf("encryptInt")

    val edf = df.withColumn(colName, encInt(df(colName)))
    edf.withColumn(colName, sha2(edf(colName), 512))
  }


  def encryptInt(col: Int): String = {
    val password = "abcdefghijklmno1".getBytes()
    val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    val keySpec = new SecretKeySpec(password, "AES")
    cipher.init(Cipher.ENCRYPT_MODE, keySpec)
    val output = Base64.encodeBase64String(cipher.doFinal(col.toString.getBytes()))
    output
  }

  def encryptStr(col: String): String = {
    val password = "abcdefghijklmno1".getBytes()
    val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    val keySpec = new SecretKeySpec(password, "AES")
    cipher.init(Cipher.ENCRYPT_MODE, keySpec)
    val output = Base64.encodeBase64String(cipher.doFinal(col.getBytes()))
    output
  }


  //  spark.udf.register("my_encrypt",encrypt _)
  //  spark.udf.register("my_decrypt",decrypt _)


}
