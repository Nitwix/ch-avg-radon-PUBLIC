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

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import main.{Symbols => s}

case class AverageFloor2004(implicit spark: SparkSession)
    extends AverageFloor(
      datasetPath =
        f"${DatasetInterface.datasetsBasePath}/avg-floor/2004/main.csv",
      schema = StructType(
        Array(
          StructField(
            s.canton,
            StringType,
            nullable = false
          ),
          StructField(s.floor, DoubleType, nullable = false)
        )
      ),
      options = DatasetInterface.unixSemiColumnCsv
    )
