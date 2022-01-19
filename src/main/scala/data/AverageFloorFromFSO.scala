/**
  * ch-avg-radon: Calcul de la moyenne de radon en suisse.
  * Copyright (C) 2022  Niels Lachat
  *
  * This program is free software: you can redistribute it and/or modify
  * it under the terms of the GNU General Public License as published by
  * the Free Software Foundation, either version 3 of the License, or
  * any later version.
  *
  * This program is distributed in the hope that it will be useful,
  * but WITHOUT ANY WARRANTY; without even the implied warranty of
  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  * GNU General Public License for more details.
  *
  * You should have received a copy of the GNU General Public License
  * along with this program.  If not, see <https://www.gnu.org/licenses/>.
  * 
  * Author: Niels Lachat <niels.lachat@bag.admin.ch>
  * 
  * For the full license, see the file `COPYING` at the root of this repository.
  */

package data

import main.{Symbols => s}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.tinylog.scala.Logger
import utils.MathUtils

import DatasetInterface.DataFrameReaderOptions
import DatasetInterface._

object AverageFloorFromFSO {

  private val cantonFullNameColumn = "CANTON_FULL_NAME"
  private val nbFloorsTextColumn = "NB_FLOORS_TEXT"
  private val nbFloorsIntColumn = "NB_FLOORS_INT"
  private val nbBuilingsColumn = "NB_BUILDINGS"
  private val nbBuildingsSumColumn = "NB_BUILDINGS_SUM"
  private val weightedNbFloorsColumn = "WEIGHTED_NB_FLOORS"
  private val weightedNbFloorsSumColumn = "WEIGHTED_NB_FLOORS_SUM"

  private val cantonFullNameToCode = Map(
    "Aargau" -> "AG",
    "Appenzell Innerrhoden" -> "AI",
    "Appenzell Ausserrhoden" -> "AR",
    "Bern / Berne" -> "BE",
    "Basel-Landschaft" -> "BL",
    "Basel-Stadt" -> "BS",
    "Fribourg / Freiburg" -> "FR",
    "Genève" -> "GE",
    "Glarus" -> "GL",
    "Graubünden / Grigioni / Grischun" -> "GR",
    "Jura" -> "JU",
    "Luzern" -> "LU",
    "Neuchâtel" -> "NE",
    "Nidwalden" -> "NW",
    "Obwalden" -> "OW",
    "St. Gallen" -> "SG",
    "Schaffhausen" -> "SH",
    "Solothurn" -> "SO",
    "Schwyz" -> "SZ",
    "Thurgau" -> "TG",
    "Ticino" -> "TI",
    "Uri" -> "UR",
    "Vaud" -> "VD",
    "Valais / Wallis" -> "VS",
    "Zug" -> "ZG",
    "Zürich" -> "ZH"
  )

  /** Get the Int representing the number of floors, from the text in the csv
    * files For example "4 ét." -> 4 or "10+ ét." -> 10
    *
    * @param nbFloorsText
    *   text representing the number of floors (as found in raw data)
    * @return
    *   the number of floors as a text
    */
  def getNbFloorsFromText(nbFloorsText: String): Int = {
    val first2Chars = nbFloorsText.take(2)
    if (first2Chars == "10") {
      return 10
    } else {
      return first2Chars.take(1).toInt
    }
  }
}

abstract class AverageFloorFromFSO(val dataYear: Int)(implicit
    spark: SparkSession
) extends AverageFloor(
      datasetPath =
        f"${DatasetInterface.datasetsBasePath}/avg-floor/$dataYear/main.csv",
      schema = StructType(
        Array(
          StructField(
            AverageFloorFromFSO.cantonFullNameColumn,
            StringType,
            nullable = false
          ),
          StructField(
            AverageFloorFromFSO.nbFloorsTextColumn,
            StringType,
            nullable = false
          ),
          StructField(
            AverageFloorFromFSO.nbBuilingsColumn,
            LongType,
            nullable = false
          )
        )
      ),
      options = DatasetInterface.unixCommaCsv
    ) {
  private val pathPrefix =
    f"${DatasetInterface.datasetsBasePath}/avg-floor/$dataYear"

  def writeToFile: Unit = {
    import spark.implicits._
    this.df
      .coalesce(1) // https://sparkbyexamples.com/spark/spark-write-dataframe-single-csv-file/
      .sort(df(s.canton))
      .map((row: Row) => // round the average floor
        (
          row.getAs[String](s.canton),
          MathUtils.roundAt(2)(row.getAs[Double](s.floor))
        )
      )
      .toDF(s.canton, s.floor)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(f"$pathPrefix/computed-avg")
  }

  override def preprocess(
      df: DataFrame
  )(implicit spark: SparkSession): DataFrame = {
    import AverageFloorFromFSO._
    import spark.implicits._
    import main.{Symbols => s}

    // clean the raw df by converting canton full name to canton code
    // and number of floors in form of text "4 ét." to Int representation
    val cleanDf = df
      .map[(String, Int, Long)]((row: Row) => {
        val rowCantonFullName = row.getAs[String](cantonFullNameColumn)
        val cantonCode = cantonFullNameToCode(rowCantonFullName)
        val nbFloors =
          getNbFloorsFromText(row.getAs[String](nbFloorsTextColumn))
        (cantonCode, nbFloors, row.getAs[Long](nbBuilingsColumn))
      })
      .toDF(s.canton, nbFloorsIntColumn, nbBuilingsColumn)

    // for each canton, get the total number of buildings
    val nbBuildingsPerCanton = cleanDf
      .groupBy(s.canton)
      .sum(nbBuilingsColumn)
      .toDF(s.canton, nbBuildingsSumColumn)

    // for each canton, get the sum of the products n * nbOfBuildingsWithNFloors for n=1..10
    // the resulting df has structure (canton, weighted sum)
    val cantonWeightedNbFloorsSum = cleanDf
      .map[(String, Long)]((row: Row) => {
        val canton = row.getAs[String](s.canton)
        val nbFloors = row.getAs[Int](nbFloorsIntColumn)
        val nbBuildings = row.getAs[Long](nbBuilingsColumn)
        // compute the products
        (canton, nbFloors * nbBuildings)
      })
      .toDF(s.canton, weightedNbFloorsColumn)
      // group by canton and sum to get (canton, weighted sum)
      .groupBy(s.canton)
      .sum(weightedNbFloorsColumn)
      .toDF(s.canton, weightedNbFloorsSumColumn)

    // join the number of buildings per canton with the weighted sum of buildings per canton on the canton column
    val cantonWithAvgFloor = nbBuildingsPerCanton
      .join(cantonWeightedNbFloorsSum, s.canton)
      .map[(String, Double)]((row: Row) => {
        val weightedNbFloorsSum = row.getAs[Long](weightedNbFloorsSumColumn)
        val nbBuildingsSum = row.getAs[Long](nbBuildingsSumColumn)
        // the average floor is the weighted sum divided by the total number of buildings
        // https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Mathematical_definition
        val avgNbFloor = weightedNbFloorsSum.toDouble / nbBuildingsSum.toDouble
        // THIS STEP IS CRITICAL: The floor where people live on average is half the number of
        // floors of the building. For example if you have a building with 3 floors, people live
        // on average on floor 1.5 .
        // "Stm das Stockwerk, in dem die Bevölkerung des entsprechenden Kantons im Mittel **wohnt**"
        val avgFloorOfHabitation = avgNbFloor / 2.0
        (row.getAs[String](s.canton), avgFloorOfHabitation)
      })
      .toDF(s.canton, s.floor)
    // cantonWithAvgFloor.collect().foreach((row:Row) => Logger.debug(row.toString()))
    cantonWithAvgFloor
  }
}
