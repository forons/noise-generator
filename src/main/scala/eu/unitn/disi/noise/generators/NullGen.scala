package eu.unitn.disi.noise.generators
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class NullGen extends INoiseGen {

  def generation[T](elem: T, rate: Double): Option[T] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    None
  }

  def generate(elem: String, rate: Double): Option[String] = {
    generation(elem, rate)
  }

  override def stringUDF(rate: Double): UserDefinedFunction = {
    udf[Option[String], String] { elem: String =>
      generate(elem, rate)
    }
  }

  def generate(elem: Int, rate: Double): Option[Int] = {
    generation(elem, rate)
  }

  override def intUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Int], Int] { elem: Int =>
      generate(elem, rate)
    }
  }

  def generate(elem: Long, rate: Double): Option[Long] = {
    generation(elem, rate)
  }

  override def longUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Long], Long] { elem: Long =>
      generate(elem, rate)
    }
  }

  def generate(elem: Double, rate: Double): Option[Double] = {
    generation(elem, rate)
  }

  override def doubleUDF(rate: Double): UserDefinedFunction = {
    udf[Option[Double], Double] { elem: Double =>
      generate(elem, rate)
    }
  }
}
