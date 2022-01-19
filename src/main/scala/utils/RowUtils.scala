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

package utils

import org.apache.spark.sql.Row

object RowUtils {

  /** Wrapper to get value in Row as an Option
    *
    * @param row
    *   Row in which we want to get a value
    * @param i
    *   Column index
    * @return
    *   Option containing the value if available
    */
  def getAsOption[T](row: Row, i: Int): Option[T] =
    if (!row.isNullAt(i))
      Some(row.getAs[T](i))
    else
      None

  /** Wrapper to get value in Row as an Option
    *
    * @param row
    *   Row in which we want to get a value
    * @param s
    *   Name of the Column / Field
    * @return
    *   Option containing the value if available
    */
  def getAsOption[T](row: Row, s: String): Option[T] =
    getAsOption(row, row.fieldIndex(s))
}
