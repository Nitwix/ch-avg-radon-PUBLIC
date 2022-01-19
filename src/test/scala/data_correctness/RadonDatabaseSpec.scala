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

package data_correctness

import baseclasses.SparkSpec
import data.RadonDatabase
import main.{Symbols => s}
import data.DatasetInterface
import main.DataExploration
import org.apache.spark.sql.Row
import utils.RowUtils

class RadonDatabaseSpec extends DataCorrectnessSpec {

  override def expectedNbOfRows: Long = 279257

  override val datasetInterface: DatasetInterface = RadonDatabase()

  override def nonNullColumns = Set()

  val (firstMeasurementId, lastMeasurementId) = (101030, 1053016)
  f"first and last values of ${s.measurementId}" should f"equal $firstMeasurementId and $lastMeasurementId respectively" in {
    assertResult(firstMeasurementId)(
      datasetInterface.df.first().getAs[Long](s.measurementId)
    )
    assertResult(lastMeasurementId)(
      datasetInterface.df.tail(1).toList.last.getAs[Long](s.measurementId)
    )
  }

  val testedRowIndex = 999
  val measurementIdOfTestedRow = 1003562
  f"The ${s.measurementId} at index $testedRowIndex" should f"equal to $measurementIdOfTestedRow" in {
    assertResult(measurementIdOfTestedRow)(
      datasetInterface.df.take(testedRowIndex+1).toList.last.getAs[Long](s.measurementId)
    )
  }

  // f"The column ${s.floor}" should "contain values that are reasonable" in {
  //   println(DataExploration.valuesOfColumn[Long](datasetInterface.df, s.floor))
  // }

  val firstEndDateString = "1982-04-27"
  val lastEndDateString = "2021-06-25"

  f"First and last measurement in db (min, max of ${s.endDate})" should f"be the values ($firstEndDateString, $lastEndDateString)" in {
    import utils.SqlDateOrdering
    import java.sql.Date
    val startDates = DataExploration.valuesOfColumn[Date](datasetInterface.df, s.endDate)
    val firstEndDate = startDates.min(SqlDateOrdering)
    val lastEndDate = startDates.max(SqlDateOrdering)
    // println(f"$firstEndDate -> $lastEndDate") // TODO WEIRD!!!
    assertResult(Date.valueOf(firstEndDateString))(firstEndDate)
    assertResult(Date.valueOf(lastEndDateString))(lastEndDate)
  }

  "[EXPL] count nb of measurements with value 0" should "_" in {
    println(datasetInterface.df
      .filter((row:Row) => {
        val concOpt = RowUtils.getAsOption[Double](row, s.rnVolConc)
        concOpt match {
          case Some(value) => value == 0
          case None => false
        }
      })
      .count())

  }
}
