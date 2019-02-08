package com.metlife.common

import com.metlife.common.model._
import spray.json.DefaultJsonProtocol
/**
  * Created by dhiraj
  */


object InputProtocolModelFormat extends DefaultJsonProtocol {



  implicit val colorFormat7b = jsonFormat2(Cast)
  implicit val colorFormat1 = jsonFormat5(DataTransformRule)
  implicit val colorFormat1jdbc = jsonFormat11(JDBCData)

  implicit val colorFormat7 = jsonFormat2(ColumnFilter)
  implicit val colorFormat6 = jsonFormat1(RecordFilter)
  implicit val colorFormat2 = jsonFormat16(FileSource)

  implicit val filterData = jsonFormat1(FilterData)
  implicit val validationSQL = jsonFormat14(ValidationStatement)

  implicit val joinJob = jsonFormat1(JoinJob)

  implicit val dataTask = jsonFormat14(Job)
  implicit val dataTaskChains = jsonFormat6(InputFlow)

}

object ResultProtocolModelFormat extends DefaultJsonProtocol {


  implicit val colorFormat7b = jsonFormat2(Cast)
  implicit val colorFormat1 = jsonFormat5(DataTransformRule)
  implicit val colorFormat1jdbc = jsonFormat11(JDBCData)
  implicit val colorFormat7 = jsonFormat2(ColumnFilter)
  implicit val colorFormat6 = jsonFormat1(RecordFilter)
  implicit val colorFormat2 = jsonFormat16(FileSource)

  implicit val columnType = jsonFormat6(ColumnType)
  implicit val filterData = jsonFormat1(FilterData)
  implicit val validationSQL = jsonFormat14(ValidationStatement)
  implicit val joinJob = jsonFormat1(JoinJob)

  implicit val dataTask = jsonFormat14(Job)
  implicit val validationSQLJson = jsonFormat11(ValidationSQL)


}




trait DiffToolJsonParser {

  import spray.json._



  def readDataTaskChainsJson(string: String): InputFlow = {
    import spray.json._
    //import MyJsonProtocolModelFormat._
    import InputProtocolModelFormat._
    val jsonAst = string.parseJson
    val js = jsonAst.convertTo[InputFlow]
    js
  }





  def readFileSourceJson(string: String): FileSource = {
    import spray.json._
    //import MyJsonProtocolModelFormat._
    import InputProtocolModelFormat._
    val jsonAst = string.parseJson
    val js = jsonAst.convertTo[FileSource]
    js
  }

  def fileSourceJson(fc: FileSource): String = {
    import spray.json._
    //import MyJsonProtocolModelFormat._
    import InputProtocolModelFormat._
    val json: JsValue = fc.toJson
    json.toString()
  }



  def validationSQLSourceJson(fc: ValidationSQL): String = {
    import spray.json._
    //import MyJsonProtocolModelFormat._
    import ResultProtocolModelFormat._
    val json: JsValue = fc.toJson
    json.toString()
  }



  def readValidationSQLJson(string: String): ValidationSQL = {
    import spray.json._
    //import MyJsonProtocolModelFormat._
    import ResultProtocolModelFormat._
    val jsonAst = string.parseJson
    val js = jsonAst.convertTo[ValidationSQL]
    js
  }




  def writeDataTaskChainsJson(fc: InputFlow): String = {
    //import ResultProtocolModelFormat._
    import InputProtocolModelFormat._
    val json = fc.toJson.prettyPrint
    // println(json)
    json
  }




}
