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

import org.apache.spark.sql.DataFrame

/** Contains symbolic values used throughout the program. Allows to catch
  * spelling mistakes in field names at compile time (and not run-time)
  */
object Symbols {
  // Column names used in the DataFrames
  val townCode = "GEMEINDENUMMER"
  val houseId = "ID_HAUS"
  val rnVolConc = "RADONKONZENTRATION_BQ_M3"
  val canton = "KANTON"
  val floor = "ETAGE"
  val measurementType = "MESSTYP"
  val roomType = "RAUMTYP"
  val peopleStay = "PERSONENAUFENTHALT"
  val validation = "VALIDIERUNG"
  val population = "POPULATION"
  val measurementId = "ID_MESSUNG"
  val startDate = "START_TIME"
  val endDate = "END_TIME"

  val floorCorrectedConc = "STOCKWERK_KORRIGIERTE_RADONKONZENTRATION_BQ_M3"
}
