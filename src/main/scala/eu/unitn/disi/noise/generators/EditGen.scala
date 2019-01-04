package eu.unitn.disi.noise.generators

import eu.unitn.disi.db.noise.generators.Edit
import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Random

class EditGen(numberOfEdits: Int = 3) extends INoiseGen {

  def deleteChar(str: String): String = {
    if (str.isEmpty) {
      str
    } else {
      val builder = new StringBuilder(str)
      builder.deleteCharAt(new Random().nextInt(str.length)).toString()
    }
  }

  def substituteChar(str: String, onlyNumbers: Boolean): String = {
    if (str.isEmpty) {
      str
    } else {
      val builder = new StringBuilder(str)
      val position = new Random().nextInt(str.length)
      var charToInsert: Char = 'a'
      do {
        if (onlyNumbers) {
          charToInsert = RandomStringUtils.randomNumeric(1).charAt(0)
        } else {
          charToInsert = RandomStringUtils.randomAlphanumeric(1).charAt(0)
        }
      } while (str.charAt(position) == charToInsert)
      builder.setCharAt(position, charToInsert)
      builder.toString()
    }
  }

  def insertChar(str: String, onlyNumbers: Boolean): String = {
    val builder = new StringBuilder(str)
    val charToInsert = if (onlyNumbers) {
      RandomStringUtils.randomNumeric(1).charAt(0)
    } else {
      RandomStringUtils.randomAlphanumeric(1).charAt(0)
    }
    if (str.isEmpty) {
      builder.insert(0, charToInsert).toString()
    } else {
      builder.insert(new Random().nextInt(str.length), charToInsert).toString()
    }
  }

  def generation(str: String, onlyNumbers: Boolean): String = {
    var support = str
    for (i <- 0 until numberOfEdits) {
      support = Edit.randomEDIT() match {
        case Edit.DELETION     => deleteChar(support)
        case Edit.SUBSTITUTION => substituteChar(support, onlyNumbers)
        case Edit.INSERTION    => insertChar(support, onlyNumbers)
      }
    }
    support
  }

  def generate(elem: String, rate: Double): Option[String] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val str = Option(elem).getOrElse(return None)
    if (str.trim.isEmpty) {
      return Some(str.trim)
    }
    Some(generation(str, onlyNumbers = false))
  }

  override def stringUDF(rate: Double): UserDefinedFunction = {
    udf[Option[String], String] { elem: String =>
      generate(elem, rate)
    }
  }

  def generate(elem: Int, rate: Double): Option[Long] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val str = Option(elem).getOrElse(return None)
    var number = str.toString.toUpperCase
    val isNegative = if (str < 0.0) {
      number = number.substring(1)
      true
    } else {
      false
    }
    number = generation(number, onlyNumbers = true)
    number = checkStringNumber(number)
    Some(if (isNegative) {
      -number.toLong
    } else {
      number.toLong
    })
  }

  override def intUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Long], Int] { elem: Int =>
      generate(elem, rate)
    }
  }

  def generate(elem: Long, rate: Double): Option[Long] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val str = Option(elem).getOrElse(return None)
    var number = str.toString.toUpperCase
    val isNegative = if (str < 0.0) {
      number = number.substring(1)
      true
    } else {
      false
    }
    number = generation(number, onlyNumbers = true)
    number = checkStringNumber(number)
    Some(if (isNegative) {
      -number.toLong
    } else {
      number.toLong
    })
  }

  override def longUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Long], Long] { elem: Long =>
      generate(elem, rate)
    }
  }

  def generate(elem: Double, rate: Double): Option[Double] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val str = Option(elem).getOrElse(return None)
    val original = str.toString.toUpperCase
    var number = original
    val isNegative = if (str < 0.0) {
      number = number.substring(1)
      true
    } else {
      false
    }
    val hasExponent = if (number.contains("E")) {
      number = number.substring(0, number.indexOf("E"))
      true
    } else {
      false
    }
    number = generation(number, onlyNumbers = true)
    number = if (hasExponent) {
      val result = number + original.substring(original.indexOf("E"))
      if (result.startsWith("E") || result.startsWith(".")) {
        "1" + result
      } else {
        result
      }
    } else {
      number
    }
    number = checkStringNumber(number)
    Some(if (isNegative) {
      -number.toDouble
    } else {
      number.toDouble
    })
  }

  override def doubleUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Double], Double] { elem: Double =>
      generate(elem, rate)
    }
  }

  def checkStringNumber(value: String): String =
    if (value.isEmpty || value.equals(".")) {
      "0"
    } else {
      value
    }
}
