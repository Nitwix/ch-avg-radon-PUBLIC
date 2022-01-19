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

import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import baseclasses.SparkSpec
import data.TownPopulationsInterface
import org.apache.spark.sql.DataFrame

class DataExplorationSpec extends SparkSpec {

  import spark.implicits._
  val simpleDF = List("a" -> 1, "b" -> 2, "c" -> 1, "d" -> 42).toDF("letter", "number")
  // val dfWithNulls = List(0 -> 3, 0 -> null, 0 -> null, 0 -> null).toDF("col1, col2")

  "valuesOfColumn" should "output exactly the set of values of a column" in {
    assertResult(Set(1,2,42)){
      DataExploration.valuesOfColumn[Int](simpleDF, "number")
    }
  }

  // this seems to be hard to test because there's no encoder for Any
  it should "not contain null values" in {
    assert(
      !DataExploration.valuesOfColumn[String](dfWithNullValues, dfWithNullValuesCols(1)).contains(null)
    )    
  }

  "countNullValues" should "return 0 for non-null column" in {
    assertResult(0){
      DataExploration.countNullValues(simpleDF, "letter")
    }
  }

  it should "return the number of null values in column" in {
    assertResult(6){
      DataExploration.countNullValues(dfWithNullValues, dfWithNullValuesCols(1))
    }
  }

  case class MockTownPopulations() extends TownPopulationsInterface {
    val df: DataFrame = List[Long](1,3,5).toDF(Symbols.townCode)
  }

  "differenceOfTownCodes" should "return the set difference between the values of town codes in rnDb and townPopulations" in {
    val mockRnDbTownCodes = List[Long](1,2,3,4,5).toDF(Symbols.townCode)
    
    assertResult(Set(2,4)){
      DataExploration.differenceOfTownCodes(mockRnDbTownCodes, MockTownPopulations())
    }
  }

  // This is cumbersome to test
  // "nbMeasurementsInInvalidTowns" 


}
