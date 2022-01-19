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

package baseclasses

import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll

abstract class SparkSpec extends AnyFlatSpec {
  val masterName = "local[*]"
  implicit val spark = SparkSession.builder().master(masterName).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") // disable all logging below ERROR

  private val dfWithNullValuesPath = "src/test/scala/test-data/ID-VALUE_with_null_values.csv"
  private val csvOptions = Map(
        "sep" -> ",", 
        "header" -> "true" // use the first line as names of columns
        )
  val dfWithNullValuesCols = List("ID", "VALUE")
  val dfWithNullValues = spark
    .read
    .schema(f"${dfWithNullValuesCols(0)} INT, ${dfWithNullValuesCols(1)} STRING")
    .options(csvOptions)
    .csv(dfWithNullValuesPath).cache()

  // override protected def afterAll(): Unit = {
  //   spark.stop()
  // }
}