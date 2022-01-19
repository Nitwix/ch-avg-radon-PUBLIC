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

package main

import data.TownPopulations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import data.RadonDatabase
import data_processing.FilteredRnDb

object Assumptions {
  /**
    * Returns true iff all columns specified by colNames in df are never null.
    *
    * @param df The DataFrame we want to check
    * @param colNames Names of the columns to check for nullity.
    * @param spark Implicit spark context
    * @return true iff all specified columns are non-null
    */
  def colsNeverNull(df: DataFrame, colNames: List[String])(implicit spark: SparkSession): Boolean = {
    import spark.implicits._
    df.map((row:Row) => 
      colNames.foldLeft(true)((curr, colName) => 
        curr && !row.isNullAt(row.fieldIndex(colName))
      )
    ).reduce((b1, b2) => b1 && b2)
  }

  /**
    * Returns true iff the set of town codes (GEMEINDENUMMER) in rnDb is a subset of the town codes in townPopulations.
    *
    * @param rnDb
    * @param townPopulations
    * @return true iff the set of town codes (GEMEINDENUMMER) in rnDb is a subset of the town codes in townPopulations.
    */
  def townCodesInRnDbSubsetOfAllTownCodes(rnDb: FilteredRnDb, townPopulations: TownPopulations): Boolean = {
    import DataExploration.differenceOfTownCodes
    differenceOfTownCodes(rnDb.df, townPopulations).isEmpty
  }

}