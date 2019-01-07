package eu.unitn.disi.noise.generators

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction

class MissingInfoGen extends INoiseGen {

  override def description(): String = {
    s"${name()} removes the input tuples"
  }

  override def name(): String = "MISSING INFO"

  override def generate(columns: Array[Int], rate: Double)(
      df: Dataset[Row]): Dataset[Row] = {
    df.filter(_ => !toGenerate(rate))
  }

  def generate(elem: String, rate: Double): Option[String] = ???

  override def stringUDF(rate: Double): UserDefinedFunction = ???

  def generate(elem: Int, rate: Double): Option[Int] = ???

  override def intUDF(rate: Double): UserDefinedFunction = ???

  def generate(elem: Long, rate: Double): Option[Long] = ???

  override def longUDF(rate: Double): UserDefinedFunction = ???

  def generate(elem: Double, rate: Double): Option[Double] = ???

  override def doubleUDF(rate: Double): UserDefinedFunction = ???
}
