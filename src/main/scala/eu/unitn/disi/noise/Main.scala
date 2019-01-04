package eu.unitn.disi.noise

import eu.unitn.disi.db.spark.io.{SparkReader, SparkWriter}
import eu.unitn.disi.noise.parser.CLParser
import eu.unitn.disi.noise.utilities.Utils
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

class Main

object Main {

  val log: Logger = LoggerFactory.getLogger(classOf[Main])

  def main(args: Array[String]): Unit = {
    val config = CLParser.parse(args)

    val spark: SparkSession =
      Utils
        .builder(s"${config.input} - ${config.noise} - ${config.rate}")
        .getOrCreate

    val start = System.currentTimeMillis()

    val df = SparkReader.read(spark,
                              config.getOptionMap,
                              config.inputFormat,
                              config.input)

    val cols = Utils.handleColumns(df, config.columns, config.idColumns)
    val outDF =
      df.transform(
        Generator
          .generateNoise(cols, config.noise, config.rate, extra = config.extra))

    SparkWriter.write(spark,
                      outDF,
                      config.getOptionMap,
                      config.output,
                      config.outputFormat)

    log.info(
      s"input:${config.input}|output:${config.output}|noise:${config.noise}|columns:{${cols
        .mkString(" ")}}|time:${System.currentTimeMillis() - start}")
  }
}
