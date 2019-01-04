package eu.unitn.disi.noise.generators

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Random

class PermutationGen(shuffle: Boolean = false) extends INoiseGen {

  def shuffle(str: String): String = {
    val chars = Random.shuffle(str.toCharArray.toSeq)
    val builder = new StringBuilder(chars.length)
    chars.foreach(builder.append)
    builder.toString()
  }

  def permutation(str: String): String = {
    val rndPosition = new Random().nextInt(str.length)
    val swapWithPrevious = if (rndPosition >= str.length - 1) {
      true
    } else {
      false
    }
    val builder = new StringBuilder
    if (swapWithPrevious) {
      builder.append(str.substring(0, rndPosition - 1))
      builder.append(str.charAt(rndPosition))
      builder.append(str.charAt(rndPosition - 1))
      builder.append(str.substring(rndPosition + 1))
    } else {
      builder.append(str.substring(0, rndPosition))
      builder.append(str.charAt(rndPosition + 1))
      builder.append(str.charAt(rndPosition))
      builder.append(str.substring(rndPosition + 2))
    }
    builder.toString()
  }

  def numberPermutation(str: String): String = {
    val builder = new StringBuilder(shuffle(str))
    if (builder.charAt(0) == '.') {
      val tmpChar = builder.charAt(1)
      builder.setCharAt(1, '.')
      builder.setCharAt(0, tmpChar)
    }
    builder.toString()
  }

  def generate(elem: String, rate: Double): Option[String] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val str = Option(elem).getOrElse(return None)
    if (str.trim.length <= 1) {
      return Some(str.trim)
    }
    Some(if (shuffle) {
      shuffle(str)
    } else {
      permutation(str)
    })
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
    val number = Option(elem).getOrElse(return None)
    val str = number.toString
    Some(if (number < 0.0) {
      -shuffle(str.substring(1)).toLong
    } else {
      shuffle(str).toLong
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
    val number = Option(elem).getOrElse(return None)
    val str = number.toString
    Some(if (number < 0.0) {
      -shuffle(str.substring(1)).toLong
    } else {
      shuffle(str).toLong
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
    val original = str.toString.toUpperCase()
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
    number = numberPermutation(number)
    if (hasExponent) {
      number = number + original.substring(original.indexOf("E"))
    }

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
}
