package eu.unitn.disi.noise.generators

import util.control.Breaks._

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Random

class AcronymGen(maxAcronymLength: Int = 4) extends INoiseGen {

  override def description(): String = {
    s"${name()} creates an acronym of maximum $maxAcronymLength words"
  }

  override def name(): String = "ACRONYM"

  def generate(elem: String, rate: Double): Option[String] = {
    if (!toGenerate(rate)) {
      return Option(elem)
    }
    val str = Option(elem).getOrElse(return None)
    if (str.trim.length <= 1) {
      return Some(str.trim)
    }
    val words = str.split("\\s", -1)
    var nonEmptyWords = 0
    for (word <- words) {
      if (!word.trim.isEmpty)
        nonEmptyWords += 1
    }
    if (words.isEmpty || nonEmptyWords == 0 || maxAcronymLength <= 0) {
      return Some(str)
    }

    var fromWordIndex = 0
    var toWordIndex = -1
    do {
      fromWordIndex = new Random().nextInt(words.length)
    } while (words(fromWordIndex).isEmpty)

    val acronymLength = Math.max(
      1,
      new Random().nextInt(Math.min(maxAcronymLength, nonEmptyWords + 1)))
    nonEmptyWords = 0
    breakable {
      for (i <- fromWordIndex until words.length) {
        if (!words(i).isEmpty) {
          nonEmptyWords += 1
          if (acronymLength <= nonEmptyWords) {
            toWordIndex = i
            break
          }
        }
      }
    }
    if (toWordIndex < 0 && fromWordIndex + acronymLength > words.length) {
      toWordIndex = words.length
    }

    val fromCharIndex = StringUtils.ordinalIndexOf(str, " ", fromWordIndex) + 1
    var toCharIndex = StringUtils.ordinalIndexOf(str, " ", toWordIndex + 1)
    toCharIndex = if (toCharIndex == -1) {
      str.length
    } else {
      toCharIndex
    }

    val builder = new StringBuilder(str.substring(0, fromCharIndex))
    nonEmptyWords = 0
    breakable {
      for (i <- fromWordIndex until words.length) {
        if (!words(i).isEmpty) {
          if (nonEmptyWords >= acronymLength) {
            break
          }
          builder.append(words(i).substring(0, 1).toUpperCase)
          nonEmptyWords += 1
        }
      }
    }
    builder.append(str.substring(toCharIndex, str.length))
    Some(builder.toString)
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
