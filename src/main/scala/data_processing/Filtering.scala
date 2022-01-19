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

import data.MeasurementTypes
import data.RadonDatabase
import data.TownPopulations
import main.CommandLineArgs
import main.DataExploration
import main.Flags
import utils.RowUtils.getAsOption
import main.{Symbols => s}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.tinylog.scala.Logger

import java.time.LocalDate
import utils.RowUtils

case class Filter(descr: String, filterFunc: Row => Boolean)

/**
 * Wrapper for the filtered radon db. 
 * This make the computation stage explicit.
 * @param df The underlying DataFrame for the radon db
 */
case class FilteredRnDb(df: DataFrame)

object Filtering {

  /** For buildings where there are measurements before and after radon
    * remediation (DE: Sanierung, FR: Assainissement), returns all the
    * measurement ids (ID_MESSUNG) of measurements that were made _before_
    * remediation.
    *
    * @param rnDb
    *   The radon database
    * @return
    *   The measurements ids of measurements made before remediation
    */
  def findIdsOfMeasurementsBeforeRemediation(
      rnDb: RadonDatabase
  )(implicit spark: SparkSession): Set[Long] = {
    import spark.implicits._
    rnDb.df
      .select(s.houseId, s.measurementId, s.measurementType)
      .groupByKey[Long]((row: Row) => row.getAs[Long](s.houseId))
      .flatMapGroups[Long](
        (houseId: Long, correspondingRows: Iterator[Row]) => {
          // need to convert to list because "one should never use an iterator after calling a method on it."
          // https://www.scala-lang.org/api/2.12.0/scala/collection/Iterator.html
          // and we need to use it multiple times
          val correspondingRowsList = correspondingRows.toList

          val hasMeasurementAfterRemediation = correspondingRowsList
            .exists(row =>
              row.getAs[String](
                s.measurementType
              ) == MeasurementTypes.afterRemediation.toString()
            )
          if (hasMeasurementAfterRemediation) {
            // need to return all ids of measurements before remediation
            correspondingRowsList
              .filter(row =>
                row.getAs[String](
                  s.measurementType
                ) == MeasurementTypes.beforeRemediation.toString()
              )
              .map(row => row.getAs[Long](s.measurementId))
          } else {
            // there is no measurement to be removed
            List()
          }
        }
      )
      .collect()
      .toSet // make a set for fast lookup (to test)
  }

  def filterRnDb(
      rnDb: RadonDatabase,
      townPopulations: TownPopulations
  )(implicit cliArgs: CommandLineArgs, spark: SparkSession): FilteredRnDb = {
    val invalidTownCodes =
      DataExploration.differenceOfTownCodes(rnDb.df, townPopulations)
    Logger.debug(f"Number of disappeared towns, |D| = ${invalidTownCodes.size}")
    val idsOfMeasurementsBeforeRemediation =
      findIdsOfMeasurementsBeforeRemediation(rnDb)

    // 2021-10-22: MGR says that we don't include rooms below 0
    val validFloorMin = 0
    val validFloorMax = 20

    val baseFilters = List[Filter](
      Filter(
        f"Keep only valid entries (${s.validation} == Y)",
        row => row.getAs[String](s.validation) == "Y"
      ),
      // 2021-12-03: Filtre probablement inutile car on a déjà
      // PERSONENAUFENTHALT == YES_LONG/SHORT
      // Filter(
      //   "Filter out any uninhabited locals",
      //   row =>
      //     row.getAs[String](s.roomType) != "Keller" &&
      //       row.getAs[String](s.roomType) != "K" &&
      //       row.getAs[String](s.roomType) != "?"
      // ),
      Filter(
        f"Keep only locals where people stay (${s.peopleStay} == YES_LONG/SHORT)",
        row => {
          val peopleStay = row.getAs[String](s.peopleStay)
          peopleStay == "YES_LONG" ||
          peopleStay == "YES_SHORT"
        }
      ),
      Filter(
        "Keep only rows with measurements",
        row => !row.isNullAt(row.fieldIndex(s.rnVolConc))
      ),
      Filter(
        "Keep only measurements done in towns with town codes that are in the 2021-07-02 OFS data for population",
        row => !invalidTownCodes.contains(row.getAs[Long](s.townCode))
      ),
      Filter(
        f"Keep only valid floor values ($validFloorMin <= ${s.floor} <= $validFloorMax)",
        row =>
          getAsOption[Int](row, row.fieldIndex(s.floor)) match {
            case Some(floor: Int) =>
              validFloorMin <= floor && floor <= validFloorMax
            case None => true
          }
      ),
      Filter(
        "Keep only measurements after remediation (for houses where there was a remediation)",
        row => {
          val measurementId = row.getAs[Long](s.measurementId)
          !idsOfMeasurementsBeforeRemediation.contains(measurementId)
        }
      ),
      Filter(
        "Keep only rows where radon concentration is > 0 Bq/m³",
        row => {
          val rnVolConc = RowUtils.getAsOption[Double](row, s.rnVolConc)
          rnVolConc match {
            case Some(value) => value > 0
            case None => true
          }
        }
      )
    )
    val keepOnlyBefore2004 = Filter(
      "Keep only measurements before 2004",
      row => {
        val maybeEndDate =
          Option(row.getAs[java.sql.Date](s.endDate))
            .map(_.toLocalDate())
        maybeEndDate match {
          case Some(date) => date.isBefore(LocalDate.of(2004, 1, 1))
          case None       => true
        }
      }
    )
    import Flags._
    val filters = if (cliArgs.isFlagEnabled(Reproduce2004Computation)) {
      keepOnlyBefore2004 :: baseFilters
    } else {
      baseFilters
    }

    if (!cliArgs.isFlagEnabled(RunFast)) {
      val filterToNbRowsRemoved = filters.map(filter =>
        (filter, rnDb.df.filter(row => !filter.filterFunc(row)).count())
      )

      filterToNbRowsRemoved.foreach { case (filter, count) =>
        Logger.debug(f"Filter '${filter.descr}' removed at most $count rows")
      }
    }

    val filteredDF = rnDb.df.filter((row: Row) => filters.forall(f => f.filterFunc(row)))
    FilteredRnDb(filteredDF)
  }
}
