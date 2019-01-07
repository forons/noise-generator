package eu.unitn.disi.noise.generators

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class BaseChangeGen(convertToBase: Int = 2) extends INoiseGen {

  override def description(): String = {
    s"${name()} changes to base $convertToBase the input element"
  }

  override def name(): String = "BASE CHANGE"

  def convertToBase(value: Double): String = {
    val maxDigits = value.toString.split("\\.")(1).length
    val builder: StringBuilder = new StringBuilder
    val newValue = if (value < 0) {
      builder.append("-")
      -value
    } else {
      value
    }
    builder.append(Integer.toString(newValue.toInt, convertToBase))
    var fractionalPart = newValue - newValue.toInt
    var digits = 0
    while ({
      fractionalPart > 0 && digits < maxDigits * Integer
        .toString(10, convertToBase)
        .length
    }) {
      if (digits == 0) builder.append(".")
      fractionalPart *= convertToBase
      builder.append(Integer.toString(fractionalPart.toInt, convertToBase))
      fractionalPart = fractionalPart - fractionalPart.toInt
      digits += 1
    }
    builder.toString
  }

  override def stringUDF(rate: Double): UserDefinedFunction = ???

  def generate(elem: Int, rate: Double): Option[String] = {
    if (!toGenerate(rate)) {
      return Option(elem.toString)
    }
    val opt = Option(elem).getOrElse(return None)
    Some(convertToBase(opt))
  }

  override def intUDF(rate: Double): UserDefinedFunction = {
    udf[Option[String], Int] { elem: Int =>
      generate(elem, rate)
    }
  }

  def generate(elem: Long, rate: Double): Option[String] = {
    if (!toGenerate(rate)) {
      return Option(elem.toString)
    }
    val opt = Option(elem).getOrElse(return None)
    Some(convertToBase(opt))
  }

  override def longUDF(rate: Double): UserDefinedFunction = {
    udf[Option[String], Long] { elem: Long =>
      generate(elem, rate)
    }
  }

  def generate(elem: Double, rate: Double): Option[String] = {
    if (!toGenerate(rate)) {
      return Option(elem.toString)
    }
    val opt = Option(elem).getOrElse(return None)
    Some(convertToBase(opt))
  }

  override def doubleUDF(rate: Double): UserDefinedFunction = {
    udf[Option[String], Double] { elem: Double =>
      generate(elem, rate)
    }
  }
}
