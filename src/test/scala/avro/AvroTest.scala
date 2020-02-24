package avro

import higherkindness.droste.data._
import io.circe.Printer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalatest.FunSuite

class AvroSuite extends FunSuite {

  val userAvro = """
                   |{
                   |   "type" : "record",
                   |   "namespace" : "avro.test",
                   |   "name" : "User",
                   |   "fields" : [
                   |      { "name" : "name" , "type" : "string" },
                   |      { "name" : "age" , "type" : "int" }
                   |   ]
                   |}
  """.stripMargin

  val recursiveUser = """
                        |{
                        |   "type" : "record",
                        |   "namespace" : "avro.test",
                        |   "name" : "User",
                        |   "fields" : [
                        |      { "name" : "name" , "type" : "string" },
                        |      { "name" : "child" , "type" : [ "null", "User"] }
                        |   ]
                        |}
                 """.stripMargin

  val jsonPrinter = Printer(
    preserveOrder = true,
    indent = "  ",
    dropNullValues = true,
    lbraceRight = "\n",
    rbraceLeft = "\n",
    lbracketRight = "\n",
    rbracketLeft = "\n",
    lrbracketsEmpty = "\n",
    arrayCommaRight = "\n",
    objectCommaRight = "\n",
    colonLeft = " ",
    colonRight = " "
  )

  def load(s: String): Fix[Avro.SchemaF] =
    Avro.loadSchema(new Schema.Parser().parse(s))

  test("Should be able to load an Avro Scheme") {
    val parsed = load(userAvro)

    info(jsonPrinter.pretty(Avro.schemaAsJson(parsed)))
  }

  test("Should be able to load a recursive Avro Scheme") {
    val parsed = load(recursiveUser)

    info(jsonPrinter.pretty(Avro.schemaAsJson(parsed)))
  }

//  test("Process Avro data") {
//    val user = new GenericData.Record(new Schema.Parser().parse(userAvro))
//
//    user.put("name", "Cacho")
//    user.put("age", 45)
//
//    info(jsonPrinter.pretty(Avro.schemaAsJson(parsed)))
//  }
}
