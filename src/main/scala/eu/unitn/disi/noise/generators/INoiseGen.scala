package eu.unitn.disi.noise.generators

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.col
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

abstract class INoiseGen extends Serializable {

  val log: Logger = LoggerFactory.getLogger(classOf[INoiseGen])

  def toGenerate(rate: Double): Boolean = {
    new Random().nextDouble() <= rate
  }

  def generate(df: Dataset[Row],
               columns: Array[Int],
               rate: Double): Dataset[Row] = {
    df.transform(generate(columns, rate))
  }

  def generate(columns: Array[Int], rate: Double)(
      df: Dataset[Row]): Dataset[Row] = {
    columns.foldLeft(df) { (_df, colIndex) =>
      val colName = df.columns(colIndex)
      _df.schema.fields(colIndex).dataType match {
        case StringType  => _df.transform(transformString(rate, colName))
        case IntegerType => _df.transform(transformInt(rate, colName))
        case LongType    => _df.transform(transformLong(rate, colName))
        case DoubleType  => _df.transform(transformDouble(rate, colName))
        case _           => ???
      }
    }
  }

  def stringUDF(rate: Double): UserDefinedFunction

  def transformString(rate: Double, column: String)(
      df: Dataset[Row]): Dataset[Row] = {
    try {
      df.withColumn(column, stringUDF(rate)(col(column)))
    } catch {
      case _: NotImplementedError =>
        log.debug(
          s"The column `$column' cannot be matched by any function of the generator")
        df
    }
  }

  def intUDF(rate: Double): UserDefinedFunction

  def transformInt(rate: Double, column: String)(
      df: Dataset[Row]): Dataset[Row] = {
    try {
      df.withColumn(column, intUDF(rate)(col(column)))
    } catch {
      case _: NotImplementedError =>
        log.debug(
          s"The column `$column' cannot be matched by any function of the generator")
        df
    }
  }

  def longUDF(rate: Double): UserDefinedFunction

  def transformLong(rate: Double, column: String)(
      df: Dataset[Row]): Dataset[Row] = {
    try {
      df.withColumn(column, longUDF(rate)(col(column)))
    } catch {
      case _: NotImplementedError =>
        log.debug(
          s"The column `$column' cannot be matched by any function of the generator")
        df
    }
  }

  def doubleUDF(rate: Double): UserDefinedFunction

  def transformDouble(rate: Double, column: String)(
      df: Dataset[Row]): Dataset[Row] = {
    try {
      df.withColumn(column, doubleUDF(rate)(col(column)))
    } catch {
      case _: NotImplementedError =>
        log.debug(
          s"The column `$column' cannot be matched by any function of the generator")
        df
    }
  }
}
