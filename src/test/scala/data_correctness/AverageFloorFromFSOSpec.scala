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

import org.scalatest.flatspec.AnyFlatSpec

class AverageFloorFromFSOSpec extends AnyFlatSpec {

  "getNbFloorsFromText" should "return the correct nb of floors" in {
    import data.AverageFloorFromFSO.getNbFloorsFromText
    assertResult(1)(getNbFloorsFromText("1 �t."))
    assertResult(5)(getNbFloorsFromText("5 �t."))
    assertResult(10)(getNbFloorsFromText("10+ �t."))
  }

}
