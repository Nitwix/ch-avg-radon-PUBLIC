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

package v01

import main.DataExploration

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import main.{Symbols => s}
import data_processing.CorrectedRnDb

object ComputeAverageVolumetricActivity {

  /** Compute the average radon concentration according to the 2004 method.
    *
    * @param rnDb
    *   Radon database (filtered for invalid data)
    * @param townPopulations
    *   Map from GEMEINDENUMMER to POPULATION
    * @return
    *   The average concentration
    */
  def computeA(corrRnDb: CorrectedRnDb, townPopulations: DataFrame)(implicit
      spark: SparkSession
  ): Double = {
    val buildingAvgs = computeBuildingAvg(corrRnDb)
    val townAvgs = computeTownAvg(buildingAvgs)
    computeAvgFromTownAvgs(townAvgs, townPopulations, corrRnDb)
  }

  /** Compute the average radon concentration for each building. The grouping is
    * done on ID_HAUS because EGID has some null values.
    *
    * @param rnDb
    * @return
    *   The processed DataFrame with the average building concentrations.
    *   RADONKONZENTRATION_BQ_M3 is the average for each building
    */
  private def computeBuildingAvg(rnDb: CorrectedRnDb): DataFrame = {
    rnDb.df
      .groupBy(s.houseId, s.townCode)
      .avg(s.floorCorrectedConc)
  }

  /** Compute the average radon concentration for each town. The grouping is
    * done on GEMEINDENUMMER
    *
    * @param buildingAvgs
    *   DataFrame containing the average for each building
    * @return
    *   The processed DataFrame with the average town concentrations.
    *   RADONKONZENTRATION_BQ_M3 is the average for each building
    */
  private def computeTownAvg(buildingAvgs: DataFrame): DataFrame = {
    buildingAvgs
      .groupBy(s.townCode)
      .avg(f"avg(${s.floorCorrectedConc})")
  }

  /** Compute the average concentration weighted by the town populations.
    *
    * @param townAvgs
    * @param townPopulations
    * @return
    */
  private def computeAvgFromTownAvgs(
      townAvgs: DataFrame,
      townPopulations: DataFrame,
      rnDb: CorrectedRnDb
  )(implicit spark: SparkSession): Double = {
    import spark.implicits._
    // sum of pop_i * town_avg
    val sumOfWeightedAvgs = townAvgs
      .join(
        townPopulations,
        s.townCode
      )
      .map((row: Row) => {
        val townAvg =
          row.getAs[Double](f"avg(avg(${s.floorCorrectedConc}))")
        val townPop = row.getAs[Long](s.population)
        townPop * townAvg
      })
      .reduce(_ + _)
    // compute sum of pop_i
    val townCodesInRnDb =
      DataExploration.valuesOfColumn[Long](rnDb.df, s.townCode)
    val sumOfPopulation = townPopulations
      .filter((row: Row) =>
        townCodesInRnDb.contains(row.getAs[Long](s.townCode))
      ) // only towns in the rnDb
      .map((row: Row) => row.getAs[Long](s.population))
      .reduce(_ + _)
    sumOfWeightedAvgs / sumOfPopulation
  }
}
