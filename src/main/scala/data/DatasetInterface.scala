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

package data

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSession, DataFrame}

object DatasetInterface {
  type DataFrameReaderOptions = Map[String, String]

  val macOSOldCsv = Map(
    "sep" -> ";",
    "header" -> "true", // use the first line as names of columns
    "lineSep" -> "\r" // because the csv's were exported on Microsoft Windows
  )
  val unixSemiColumnCsv = Map(
    "sep" -> ";",
    "header" -> "true",
    "lineSep" -> "\n"
  )

  val unixCommaCsv = Map(
    "sep" -> ",",
    "header" -> "true",
    "lineSep" -> "\n"
  )

  val datasetsBasePath = "data"
}

/** Wraps a DataFrame to make explicit what can be expected to be found in the
  * wrapped DataFrame.
  */
trait DataFrameWrapper {
  val df: DataFrame
}


abstract class DatasetInterface(
    datasetPath: String,
    schema: StructType,
    options: DatasetInterface.DataFrameReaderOptions
)(implicit spark: SparkSession)
    extends DataFrameWrapper {

  /** The loaded DataFrame First access is slow (lazy val)
    */
  lazy val df = readData

  /** Override this method to perform some preprocessing on the data. This is
    * called just after the data is loaded
    *
    * @param df
    * @return
    *   the processed DataFrame
    */
  def preprocess(df: DataFrame)(implicit spark: SparkSession): DataFrame = df

  private def readData(implicit spark: SparkSession): DataFrame = {
    val df = spark.read
      .schema(schema)
      .options(options)
      .csv(datasetPath)
      .cache()
    preprocess(df)
  }
}
