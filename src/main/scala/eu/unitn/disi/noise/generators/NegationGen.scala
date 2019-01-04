package eu.unitn.disi.noise.generators

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class NegationGen extends INoiseGen {

  override def stringUDF(rate: Double): UserDefinedFunction = ???

  def generate(elem: Int, rate: Double): Option[Int] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val opt = Option(elem).getOrElse(return None)
    Some(-opt)
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
    val opt = Option(elem).getOrElse(return None)
    Some(-opt)
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
    val opt = Option(elem).getOrElse(return None)
    Some(-opt)
  }

  override def doubleUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Double], Double] { elem: Double =>
      generate(elem, rate)
    }
  }
}
