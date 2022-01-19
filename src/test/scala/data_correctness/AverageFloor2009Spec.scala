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
import data.AverageFloor2009
import data.DatasetInterface
import main.DataExploration
import main.{Symbols => s}

class AverageFloor2009Spec extends DataCorrectnessSpec {

  override def expectedNbOfRows: Long = 26

  override val datasetInterface: DatasetInterface = AverageFloor2009()

  override def nonNullColumns: Set[String] = Set(s.canton, s.floor)

  "toMap" should "contain entries with correct values for NE, TI, BE" in {
    val avgFloorMap = AverageFloor2009().toMap
    // avgFloorMap.toList.sortBy{case (canton, floor) => canton}.foreach(println)
    assertResult(1.4787306217985685)(avgFloorMap("NE"))
    assertResult(1.1311385310107842)(avgFloorMap("TI"))
    assertResult(1.2719479665125668)(avgFloorMap("BE"))
  }

  // "writeToFile" should "write to the correct file (MANUAL CHECK)" in {
  //   AverageFloor2009().writeToFile
  // }
}
