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

package data_processing

import data.AverageFloor
import data.RadonDatabase
import utils.RowUtils
import main.Symbols
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Wrapper for the corrected radon db. 
 * This make the computation stage explicit.
 * @param df The underlying DataFrame for the radon db
 */
case class CorrectedRnDb(df: DataFrame)

object Corrections {
  @deprecated(
    "This is not needed anymore, the values in the db are already corrected"
  )
  def correctForSeason = ???

  /** Applies the correction for the floor of the measurement
    *
    * @param rnDb
    *   The original radon db
    * @param avgFloor
    *   DataFrame containing the floor for each
    * @param spark
    * @return
    */
  def correctForFloor(rnDb: FilteredRnDb, avgFloor: AverageFloor)(implicit
      spark: SparkSession
  ): CorrectedRnDb = {
    import spark.implicits._
    def correctionFunction(
        measuredConcentration: Double,
        cantonAvgFloor: Double,
        floorOption: Option[Int]
    ): Double = {
      import Math.exp
      floorOption match {
        case Some(floor) => {
          val expConst = -0.19

          measuredConcentration * exp(
            expConst * (cantonAvgFloor - floor)
          ) // see README.md for reference

        }
        case None => measuredConcentration
      }
    }

    val avgFloorMap = avgFloor.toMap
    val floorCorrectedConc = rnDb.df
      .map[(Long, Double)]((row: Row) => {
        val canton = row.getAs[String](Symbols.canton)
        val cantonAvgFloor = avgFloorMap(canton)
        val floorOption =
          RowUtils.getAsOption[Int](row, row.fieldIndex(Symbols.floor))
        val measuredConcentration = row.getAs[Double](Symbols.rnVolConc)
        val measurementId = row.getAs[Long](Symbols.measurementId)
        (
          measurementId,
          correctionFunction(measuredConcentration, cantonAvgFloor, floorOption)
        )
      })
      .toDF(Symbols.measurementId, Symbols.floorCorrectedConc)

    val correctedDF = rnDb.df
      .drop(
        Symbols.rnVolConc
      ) // drop the uncorrected measurements to prevent use after this point
      .join(floorCorrectedConc, Symbols.measurementId)
    CorrectedRnDb(correctedDF)
  }
}
