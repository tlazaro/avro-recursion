package avro

import java.io.ByteArrayOutputStream
import java.util

import higherkindness.droste.data._
import io.circe.Printer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.scalatest.FunSuite

class AvroSuite extends FunSuite {

  val userAvro = """
                   |{
                   |   "type" : "record",
                   |   "namespace" : "avro.test",
                   |   "name" : "User",
                   |   "fields" : [
                   |      { "name" : "name" , "type" : "string" },
                   |      { "name" : "age" , "type" : ["null", "int", "string"] },
                   |      { "name" : "friends" , "type" : { "type": "array", "items": "User" } }
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

  def load(s: String): Fix[SchemaF] =
    SchemaF.loadSchema(new Schema.Parser().parse(s))

  test("Should be able to load an Avro Scheme") {
    val parsed = load(userAvro)

    info(jsonPrinter.pretty(SchemaF.schemaAsJson(parsed)))
  }

  test("Should be able to load a recursive Avro Scheme") {
    val parsed = load(recursiveUser)

    info(jsonPrinter.pretty(SchemaF.schemaAsJson(parsed)))
  }

  def getUser: GenericRecord = {
    val user = new GenericData.Record(new Schema.Parser().parse(userAvro))

    user.put("name", "Cacho")
    user.put("age", 45)
    user.put("friends", new util.ArrayList[GenericRecord]())

    user
  }

  test("Process Avro data") {
    val user = getUser

    val baos    = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().jsonEncoder(user.getSchema, baos, true)
    val writer  = new GenericDatumWriter[GenericRecord](user.getSchema)

    writer.write(user, encoder)
    encoder.flush()

    info("\n" + baos.toString("UTF-8"))
  }

  test("Process Avro data with recursion schemes") {
    val user = getUser

    val result = DatumF.writeGenericRecord(user, Map.empty)

    result match {
      case Left(value) => fail(s"Failed to process Avro record: $value")
      case Right(json) => info(jsonPrinter.pretty(json))
    }
  }
}
