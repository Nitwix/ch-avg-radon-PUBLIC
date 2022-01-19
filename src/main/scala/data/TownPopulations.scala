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
import DatasetInterface._

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import main.{Symbols => s}
import org.apache.spark.sql.functions.col

trait TownPopulationsInterface extends DataFrameWrapper

case class TownPopulations(implicit spark:SparkSession) extends DatasetInterface(
  datasetPath = f"${datasetsBasePath}/population-per-town/main.csv",
  schema = StructType(Array(
    StructField(s.townCode, DoubleType, nullable = false),
    StructField(s.population, DoubleType, nullable = false)
  )),
  options = DatasetInterface.unixCommaCsv
) with TownPopulationsInterface {
  override def preprocess(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // we need to import as Double and cast as Long because OpenRefine seems to automatically
    // parse numbers and export them with a trailing ".0"
    val withTownCodeCast = df.withColumn(s.townCode, col(s.townCode).cast(LongType))
    val withBothCast = withTownCodeCast.withColumn(s.population, col(s.population).cast(LongType))
    withBothCast
  }
}