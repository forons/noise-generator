package eu.unitn.disi.noise

import eu.unitn.disi.db.noise.NoiseType
import eu.unitn.disi.noise.generators._
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}

class Generator

object Generator {

  val log: Logger = LoggerFactory.getLogger(classOf[Generator])

  def generate(df: Dataset[Row],
               columns: Array[Int],
               noise: NoiseType,
               rate: Double,
               extra: Any = null): Dataset[Row] = {
    df.transform(generateNoise(columns, noise, rate))
  }

  def generateNoise(columns: Array[Int],
                    noise: NoiseType,
                    rate: Double,
                    extra: Any = null)(df: Dataset[Row]): Dataset[Row] = {
    try {
      val generator: INoiseGen = noise match {
        case NoiseType.ABBREVIATION =>
          new AbbreviationGen()
        case NoiseType.ACRONYM =>
          if (extra == null) {
            new AcronymGen(maxAcronymLength = 4)
          } else {
            new AcronymGen(maxAcronymLength = extra.asInstanceOf[Int])
          }
        case NoiseType.BASE_CHANGE =>
          if (extra == null) {
            new BaseChangeGen(convertToBase = 2)
          } else {
            new BaseChangeGen(convertToBase = extra.asInstanceOf[Int])
          }
        case NoiseType.EDIT =>
          if (extra == null) {
            new EditGen(numberOfEdits = 3)
          } else {
            new EditGen(numberOfEdits = extra.asInstanceOf[Int])
          }
        case NoiseType.MISSING_INFO =>
          new MissingInfoGen()
        case NoiseType.MULTILINGUAL => ???
        case NoiseType.NEGATION =>
          new NegationGen()
        case NoiseType.NULL =>
          new NullGen()
        case NoiseType.PERMUTATION =>
          if (extra == null) {
            new PermutationGen(shuffle = false)
          } else {
            new PermutationGen(shuffle = extra.asInstanceOf[Boolean])
          }
        case NoiseType.SCALE =>
          if (extra == null) {
            new ScaleGen(scaleFactor = 10.0)
          } else {
            new ScaleGen(scaleFactor = extra.asInstanceOf[Double])
          }
        case NoiseType.SHUFFLING =>
          if (extra == null) {
            new ShufflingGen(numberOfShuffles = 3)
          } else {
            new ShufflingGen(numberOfShuffles = extra.asInstanceOf[Int])
          }
        case NoiseType.SYNONYM => ???
      }
      df.transform(generator.generate(columns, rate))
    } catch {
      case _: NotImplementedError =>
        log.error(s"The `${noise.toString}' is not yet implemented")
        df
    }
  }

  def getNoise(noise: NoiseType): INoiseGen = {
    try {
      noise match {
        case NoiseType.ABBREVIATION =>
          new AbbreviationGen()
        case NoiseType.ACRONYM =>
          new AcronymGen(maxAcronymLength = 4)
        case NoiseType.BASE_CHANGE =>
          new BaseChangeGen(convertToBase = 2)
        case NoiseType.EDIT =>
          new EditGen(numberOfEdits = 3)
        case NoiseType.MISSING_INFO =>
          new MissingInfoGen()
        case NoiseType.MULTILINGUAL => ???
        case NoiseType.NEGATION =>
          new NegationGen()
        case NoiseType.NULL =>
          new NullGen()
        case NoiseType.PERMUTATION =>
          new PermutationGen(shuffle = false)
        case NoiseType.SCALE =>
          new ScaleGen(scaleFactor = 10.0)
        case NoiseType.SHUFFLING =>
          new ShufflingGen(numberOfShuffles = 3)
        case NoiseType.SYNONYM => ???
      }
    } catch {
      case _: NotImplementedError =>
        log.error(s"The `${noise.toString}' is not yet implemented")
        null
    }
  }

  def generateSingleNoise
}
