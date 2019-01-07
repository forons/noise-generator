package eu.unitn.disi.noise.generators

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class AbbreviationGen extends INoiseGen {

  override def description(): String = {
    s"${name()} keeps only the first char or digit of the input value"
  }

  override def name(): String = "ABBREVIATION"

  def generate(elem: String, rate: Double): Option[String] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val str = Option(elem).getOrElse(return None)
    Some(str.substring(0, 1))
  }

  override def stringUDF(rate: Double): UserDefinedFunction = {
    udf[Option[String], String] { elem: String =>
      generate(elem, rate)
    }
  }

  def generate(elem: Int, rate: Double): Option[Int] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val num = Option(elem).getOrElse(return None)
    if (num < 0) {
      Some(num.toString.substring(0, 2).toInt)
    } else {
      Some(num.toString.substring(0, 1).toInt)
    }
  }

  override def intUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Int], Int] { elem: Int =>
      generate(elem, rate)
    }
  }

  def generate(elem: Long, rate: Double): Option[Long] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val num = Option(elem).getOrElse(return None)
    if (num < 0) {
      Some(num.toString.substring(0, 2).toLong)
    } else {
      Some(num.toString.substring(0, 1).toLong)
    }
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
    val num = Option(elem).getOrElse(return None)
    if (num < 0) {
      Some(num.toString.substring(0, 2).toDouble)
    } else {
      Some(num.toString.substring(0, 1).toDouble)
    }
  }

  override def doubleUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Double], Double] { elem: Double =>
      generate(elem, rate)
    }
  }
}
