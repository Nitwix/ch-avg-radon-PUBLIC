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

import data.AverageFloor
import data.AverageFloor2004
import data.AverageFloor2020
import data.RadonDatabase
import data.TownPopulations
import main.Assumptions
import main.CommandLineArgs
import main.Flags
import main.{Symbols => s}
import org.apache.spark.sql.SparkSession
import org.tinylog.scala.Logger
import data_processing.{Filtering, Corrections}

/**
  * Contains static methods related to the computation of version 01 of the average.
  */
object MainV01 {
  /**
    * Returns the average radon concentration in Bq/m^3, according to v01 of the computation.
    *
    * @param spark SparkSession required for spark
    * @param cliArgs CommandLineArgs, as parsed from main()
    * @return Average radon concentration in Bq/m^3
    */
  def computeV01(implicit
      spark: SparkSession,
      cliArgs: CommandLineArgs
  ): Double = {
    val avgFloor: AverageFloor =
      if (cliArgs.isFlagEnabled(Flags.Reproduce2004Computation)) {
        AverageFloor2004()
      } else {
        AverageFloor2020()
      }
    val rawRnDb = RadonDatabase()
    val townPopulations = TownPopulations()

    rawRnDb.df.printSchema()

    Logger.info(f"Count rn db before filtering: ${rawRnDb.df.count()}")
    val filteredRnDb =
      Filtering.filterRnDb(rawRnDb, townPopulations)
    Logger.info(f"Count rn db after filtering: ${filteredRnDb.df.count()}")

    if (!cliArgs.isFlagEnabled(Flags.RunFast)) {
      assert(
        Assumptions.colsNeverNull(
          filteredRnDb.df,
          List(
            s.houseId,
            s.rnVolConc,
            s.townCode,
            s.canton,
            s.measurementId
          )
        )
      )
      assert(
        Assumptions.townCodesInRnDbSubsetOfAllTownCodes(
          filteredRnDb,
          townPopulations
        )
      )
    }

    val correctedRnDb = Corrections.correctForFloor(filteredRnDb, avgFloor)

    ComputeAverageVolumetricActivity.computeA(correctedRnDb, townPopulations.df)
  }
}
