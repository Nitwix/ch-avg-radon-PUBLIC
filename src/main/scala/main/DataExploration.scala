/** ch-avg-radon: Calcul de la moyenne de radon en suisse. Copyright (C) 2022
  * Niels Lachat
  *
  * This program is free software: you can redistribute it and/or modify it
  * under the terms of the GNU General Public License as published by the Free
  * Software Foundation, either version 3 of the License, or any later version.
  *
  * This program is distributed in the hope that it will be useful, but WITHOUT
  * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
  * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
  * more details.
  *
  * You should have received a copy of the GNU General Public License along with
  * this program. If not, see <https://www.gnu.org/licenses/>.
  *
  * Author: Niels Lachat <niels.lachat@bag.admin.ch>
  *
  * For the full license, see the file `COPYING` at the root of this repository.
  */

package main

import org.apache.spark.sql._
import utils.RowUtils.getAsOption
import org.tinylog.scala.Logger
import data.RadonDatabase
import data.TownPopulations
import data_processing.FilteredRnDb
import data.TownPopulationsInterface

/** Provides static methods to explore data stored in DataFrames
  */
object DataExploration {

  /** Returns all the values that colName takes in the given DataFrame
    *
    * @param df
    *   DataFrame
    * @return
    *   the values taken by colName in df
    */
  def valuesOfColumn[A](df: DataFrame, colName: String): Set[A] =
    df
      .select(colName)
      .dropDuplicates()
      .collect()
      .toSet
      .filter((row: Row) => !row.isNullAt(0))
      .map((row: Row) => row.getAs[A](colName))

  /** Counts the number of null values in the given column of the given
    * DataFrame
    *
    * @param df
    *   DataFrame
    * @return
    *   Count of null values
    */
  def countNullValues(df: DataFrame, colName: String): Long =
    df
      .select(colName)
      .filter((row: Row) => row.isNullAt(0))
      .count()

  /** Returns the set difference : TownCodes(rnDb) - TownCodes(townPopulations)
    * This should ideally be empty because for each mesurement in rnDb we need
    * the population of the town to be able to compute the weighted average.
    *
    * @param rnDb
    *   The radon db. MUST HAVE a column called `Symbols.townCode`
    * @param townPopulations
    *   the DataFrame containing the population for each town. MUST HAVE a
    *   column called `Symbols.townCode`
    * @return
    *   The set difference
    */
  def differenceOfTownCodes(
      rnDb: DataFrame,
      townPopulations: TownPopulationsInterface
  ): Set[Long] = {
    val rnDbTownCodes = valuesOfColumn[Long](rnDb, Symbols.townCode)
    val townPopulationsTownCodes =
      valuesOfColumn[Long](townPopulations.df, Symbols.townCode)

    rnDbTownCodes -- townPopulationsTownCodes
  }

  /** Returns the nb of measurements done in "invalid towns". Invalid towns are
    * defined to be the towns that have disappeared (due to commune fusions) and
    * for which we have no population data.
    *
    * @param rnDb
    *   DataFrame representing the radon db. MUST HAVE a column called
    *   `Symbols.townCode`
    * @param townPopulations
    *   DataFrame representing the radon db. MUST HAVE a column called
    *   `Symbols.townCode`
    * @return
    */
  def nbMeasurementsInInvalidTowns(
      rnDb: DataFrame,
      townPopulations: TownPopulations
  ): Long = {
    val invalidTownCodes = differenceOfTownCodes(rnDb, townPopulations)
    rnDb
      .filter((row: Row) =>
        invalidTownCodes.contains(row.getAs[Long](Symbols.townCode))
      )
      .count()
  }

  /** Returns the range of values of a column in a DataFrame, given an ordering
    *
    * @param df
    *   DataFrame in which the column is
    * @param colName
    *   Name of the column for which to find the range
    * @param ord
    *   Implicit ordering for values in the column
    * @return
    *   A pair with the minimum and maximum of the column, respectively
    */
  def rangeOfColumn[A](df: DataFrame, colName: String)(implicit
      ord: Ordering[A]
  ): (A, A) = {
    val setOfValues = valuesOfColumn[A](df, colName)
    (setOfValues.min, setOfValues.max)
  }
}
