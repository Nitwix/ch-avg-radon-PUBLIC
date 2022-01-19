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

package data_processing

import baseclasses.SparkSpec
import data.RadonDatabase
import org.apache.spark.sql.types._
import main.{Symbols => s}

class FilteringSpec extends SparkSpec {
  "findIdsOfMeasurementsBeforeRemediation" should "return the correct ids for a mock Rn database" in {
    val mockRnDb = RadonDatabase(
      providedDatasetPath = "src/test/scala/test-data/rn-db-MESSTYP.csv",
      providedSchema = StructType(
        Array(
          StructField(s.houseId, LongType, false),
          StructField(s.measurementId, LongType, false),
          StructField(s.measurementType, StringType, true)
        )
      )
    )
    assertResult(Set(6, 8))(
      Filtering.findIdsOfMeasurementsBeforeRemediation(mockRnDb)
    )
  }
}
