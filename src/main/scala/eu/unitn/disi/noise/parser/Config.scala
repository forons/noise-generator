package eu.unitn.disi.noise.parser

import eu.unitn.disi.db.noise.NoiseType
import eu.unitn.disi.db.spark.io.Format

case class Config(input: String,
                  inputFormat: Format = Format.CSV,
                  output: String,
                  outputFormat: Format = Format.CSV,
                  nullValues: String = "",
                  quote: String = "\"",
                  inferSchema: Boolean = true,
                  hasHeader: Boolean = true,
                  noise: NoiseType,
                  rate: Double,
                  columns: Array[Int],
                  idColumns: Array[Int],
                  extra: Any = null) {

  def getOptionMap: Map[String, String] = {
    Map[String, String]("inferSchema" -> inferSchema.toString,
                        "header" -> hasHeader.toString,
                        "quote" -> quote,
                        "nullValue" -> nullValues)
  }
}
