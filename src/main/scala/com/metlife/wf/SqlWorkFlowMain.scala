package com.metlife.wf

import java.io.ByteArrayOutputStream

import com.metlife.common.{DiffToolJsonParser, JobsExecutor}
import com.metlife.common.model.InputFlow
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.wf.util.FileUtil

/**
  * Created by dhiraj
  */
object SqlWorkFlowMain extends SparkInit with DiffToolJsonParser {


  var submittedTime: Option[Long] = None
  var LICENSED: Boolean = false
  var jobId: String = null
  var execId: String = null
  var projectId: String = null
  //  var base64enCoded: Boolean = false
  var exceptionOccured: Boolean = false

  def setSparkContext(lsparkContext: SparkSession): Unit = {
    sparkContextLivy = lsparkContext
  }

  var sparkContextLivy: SparkSession = null

  def main(args: Array[String]) {

    System.out.println("args = " + args.toList)
    val argsMap = parseArgs(args)

    val inputDiffPropsFile = argsMap.getOrElse("INPUT_FILE", "")

    val debug = if (argsMap.getOrElse("DEBUG", "false") == "true") true else false
    val local = if (argsMap.getOrElse("local", "false") == "true") true else false
    val spark = if (sparkContextLivy != null) {
      sparkContextLivy
    } else if (local == false) {
      SparkSession.builder().appName("SQLWorkFlow").enableHiveSupport().getOrCreate()
    }
    else {
      SparkSession.builder().appName("SQLWorkFlow").master("local").getOrCreate()
    }

    val sQLContext = spark.sqlContext

    import java.util.Properties

    import org.apache.log4j.PropertyConfigurator

    val props = new Properties()
    props.load(getClass.getClassLoader.getResourceAsStream("log4j.properties"))
    PropertyConfigurator.configure(props)
    val hdfsOutput: Boolean = if (argsMap.getOrElse("HDFS_OUTPUT", "true") == "true") true else false

    try {
      processFlow(inputDiffPropsFile, sQLContext, hdfsOutput, debug)
    } catch {
      case e: Exception => e.printStackTrace(); throw e;
    } finally {
      spark.stop()
    }
  }


  def processFlow(inputFile: String, sqlContext: SQLContext, hdfsOutput: Boolean = true, debug: Boolean): Unit = {
    val jsonContent = if (hdfsOutput) org.wf.util.FileUtil.getFileContent(inputFile, sqlContext.sparkContext.hadoopConfiguration) else Some(scala.io.Source.fromFile(inputFile).getLines().mkString)
    val filesCompare: InputFlow = if (jsonContent.isDefined) readDataTaskChainsJson(jsonContent.get) else throw new IllegalArgumentException("unable to read input JSON " + inputFile)
    new JobsExecutor(filesCompare, sqlContext, debug).processChains()

  }

  def writeToFile(jsonStr: String, filePath: Option[String], hdfsOutput: Boolean, sqlContext: SQLContext): Unit = {
    if (filePath.isDefined) {
      if (hdfsOutput) FileUtil.writeToFile(filePath, jsonStr, sqlContext.sparkContext.hadoopConfiguration) else FileUtil.writeToTextFile(filePath.get, jsonStr)
    }
  }

  def compress(input: String): Array[Byte] = {

    import java.util.zip.{ZipEntry, ZipOutputStream}

    val path = System.getProperty("java.io.tmpdir") + "/" + "flow_result.json"
    val baos = new ByteArrayOutputStream()
    val zos = new ZipOutputStream(baos)
    /* File is not on the disk, test.txt indicates
        only the file name to be put into the zip */
    val entry = new ZipEntry("flow_result.json")
    zos.putNextEntry(entry)
    zos.write(input.getBytes)
    zos.closeEntry()
    baos.toByteArray

  }


  def parseArgsJava(args: Array[String]): java.util.Map[String, String] = {
    import scala.collection.JavaConverters._
    parseArgs(args).asJava
  }

  def parseArgs(args: Array[String]): scala.collection.Map[String, String] = {
    val v: scala.collection.Map[String, String] = args.filter(a => a.contains("=")).map(a => {
      val ar = a.splitAt(a.indexOf("="))
      val res = ar._1 -> ar._2.substring(1, ar._2.length)

      res
    }).toMap
    v
  }


//  def csvToWF(filePath: String, sqlContext: SQLContext, local: Boolean): InputFlow = {
//    //    sqlContext.read.format()
//    import CSV_INPUT_JOB._
//    import sqlContext.implicits._
//    val ds = sqlContext.read.option("header", "true").option("delimiter", ",").csv(filePath).as[CSV_INPUT_JOB] // //.save(outputDir)
//
//
//    def getSqlText(filePath: String, actSql: String) = {
//      val sql: Option[String] = if (filePath != null && filePath.length > 0) {
//        if (local) Some(new String(Files.readAllBytes(Paths.get(filePath)))) else FileUtil.getFileContent(filePath, sqlContext.sparkContext.hadoopConfiguration)
//      } else Some(actSql)
//      sql
//    }
//
//    val jobsMap = ds.collect().map(record => {
//      val depends_on: Option[List[String]] = if (record != null || record.depends_on.length == 0) None else Some(record.depends_on.split(":").toList)
//      val outputName: Option[String] = if (record.output_tbl_name != null) Some(record.job_name) else None
//      record.job_type match {
//        case SqlJobTypes.INPUT_SOURCE => {
//
//        }
//        case SqlJobTypes.OUTPUT_SOURCE => {
//
//        }
//        case SqlJobTypes.TRANSFORMATION => {
//          val sql: Option[String] = getSqlText(record.transformation_file, record.transformation_sql)
//          record.job_name -> Job(record.job_name, Some(true), depends_on, None, None, Some(List(DataTransformRule(transformSQL = sql))), jobOutputTableName = outputName)
//        }
//        case SqlJobTypes.FILTER => {
//          val sql: Option[String] = getSqlText(record.filter_file, record.filter_sql)
//          record.job_name -> Job(record.job_name, Some(true), depends_on, None, filterData = Some(FilterData(sql)), None, None, jobOutputTableName = outputName)
//        }
//        case SqlJobTypes.JOIN => {
//          val sql: Option[String] = getSqlText(record.join_file, record.join_sql)
//          record.job_name -> Job(record.job_name, Some(true), depends_on, None, joins = Some(JoinJob(sql)), jobOutputTableName = outputName)
//        }
//        case _ => None
//
//      }
//    })
//  }

}
