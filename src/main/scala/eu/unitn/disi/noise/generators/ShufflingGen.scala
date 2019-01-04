package eu.unitn.disi.noise.generators

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Random

class ShufflingGen(numberOfShuffles: Int = 3) extends INoiseGen {

  def generate(elem: String, rate: Double): Option[String] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val str = Option(elem).getOrElse(return None)
    if (str.isEmpty || str.trim.length == 1) {
      return Some(str)
    }
    val words = elem.split("\\s", -1)
    var nonEmptyWords = 0
    for (word <- words) {
      nonEmptyWords = if (!word.isEmpty) {
        nonEmptyWords + 1
      } else {
        nonEmptyWords
      }
    }
    if (words.length <= 1 || nonEmptyWords <= 1) {
      return Some(elem)
    }
    for (shuffle <- 0 until numberOfShuffles) {
      var firstWordIndex = 0
      do {
        firstWordIndex = (new Random).nextInt(words.length)
      } while (words(firstWordIndex).isEmpty)
      var precedentIndex = -1
      var succeedingIndex = -1
      var i = 0
      for (i <- 0 until Math.max(firstWordIndex, words.length - firstWordIndex)) {
        if ((firstWordIndex + i < words.length) && !words(firstWordIndex + i).isEmpty)
          succeedingIndex = firstWordIndex + i
        if ((firstWordIndex - i > 0) && !words(firstWordIndex - 1).isEmpty) {
          precedentIndex = firstWordIndex - i
        }
      }
      var swapWithIndex = 0
      if (precedentIndex >= 0 && succeedingIndex >= 0) {
        swapWithIndex = if (new Random().nextBoolean) {
          precedentIndex
        } else {
          succeedingIndex
        }
      } else {
        swapWithIndex = if (precedentIndex >= 0) {
          precedentIndex
        } else {
          succeedingIndex
        }
      }
      val tmp = words(firstWordIndex)
      words(firstWordIndex) = words(swapWithIndex)
      words(swapWithIndex) = tmp
    }
    Some(words.mkString(" "))
  }

  override def stringUDF(rate: Double): UserDefinedFunction = {
    udf[Option[String], String] { elem: String =>
      generate(elem, rate)
    }
  }

  override def intUDF(rate: Double): UserDefinedFunction = ???

  override def longUDF(rate: Double): UserDefinedFunction = ???

  override def doubleUDF(rate: Double): UserDefinedFunction = ???
}
