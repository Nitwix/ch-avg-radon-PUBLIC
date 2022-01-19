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

import utils.RowUtils
import main.{Symbols => s}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.tinylog.scala.Logger

import DatasetInterface._

object RadonDatabase {

  /** to get this dataset from the raw Rn database, use
    * metadata/rn-db-UNIX_CSV_UTF8_PARTIAL-openrefine-options.json with
    * OpenRefine (https://openrefine.org/)
    */
  val datasetPath =
    f"${datasetsBasePath}/rn-db/main.csv"
  val schema = StructType(
    Array(
      StructField(s.canton, StringType, false),
      StructField(s.houseId, LongType, false),
      StructField("EGID", LongType, true),
      // StructField("PARZELLENNUMMER", StringType, true),
      // StructField("VORNAME", StringType, true),
      // StructField("NAME", StringType, true),
      // StructField("FUNKTION", StringType, true),
      // StructField("FIRMA", StringType, true),
      // StructField("ADDRESSTYP", StringType, true),
      // StructField("STRASSE_NR", StringType, true),
      // StructField("GEBAEUDEBEZEICHNUNG", StringType, true),
      // StructField("POSTLEITZAHL", StringType, true),
      // StructField("ORT", StringType, true),
      // StructField("ADRESSE_ALT", StringType, true),
      StructField(s.townCode, LongType, true),
      // StructField("GEMEINDE", StringType, true),
      // StructField("GEBAEUDEKATEGORIE", StringType, true),
      // StructField("BAUJAHR", StringType, true),
      // StructField("Anzahl_Etagen", IntegerType, true),
      // StructField("Fundament", StringType, true),
      // StructField("Struktur_des_Fundaments", StringType, true),
      // StructField("Untergeschoss vorhanden", StringType, true),
      // StructField("Hanglage", StringType, true),
      // StructField("Kontrollierte_Lftung", StringType, true),
      // StructField("KOORDINATENBEZUG", StringType, true),
      // StructField("LV95_ORDINATE", StringType, true),
      // StructField("LV95_ABSZISSE", StringType, true),
      // StructField("LV03_ORDINATE", StringType, true),
      // StructField("LV03_ABSZISSE", StringType, true),
      // StructField("BEMERKUNG_HAUS", StringType, true),
      StructField(s.measurementId, LongType, false),
      // StructField("MESSPROTOKOLL", StringType, true),
      StructField(s.measurementType, StringType, true),
      // StructField("DOSIMETER_NO", StringType, true),
      // StructField("IDENTIFIER", StringType, true),
      StructField(s.startDate, DateType, true),
      StructField(s.endDate, DateType, true),
      // StructField("KEYWORDS", StringType, true),
      // StructField("ID_Raum", StringType, true),
      StructField(s.roomType, StringType, true),
      // StructField("RAUMBEZEICHNUNG", StringType, true),
      StructField(s.peopleStay, StringType, true),
      StructField(s.floor, IntegerType, true),
      StructField("RADONEXPOSITION_KBQH_M3", StringType, true),
      StructField(s.rnVolConc, DoubleType, true),
      // StructField("MESSUNSICHERHEIT", StringType, true),
      StructField(s.validation, StringType, true)
      // StructField("SANIERUNGSFRIST_MESSUNG", StringType, true),
      // StructField("BEMERKUNG", StringType, true),
      // StructField("BENUTZER", StringType, true)
    )
  )
}

case class RadonDatabase(
    // allows inserting mock test data during testing
    providedDatasetPath: String = RadonDatabase.datasetPath,
    providedSchema: StructType = RadonDatabase.schema
)(implicit spark: SparkSession)
    extends DatasetInterface(
      datasetPath = providedDatasetPath,
      schema = providedSchema,
      options = Map(
        "sep" -> ",",
        "header" -> "true",
        "lineSep" -> "\n",
        "dateFormat" -> "dd.MM.yyyy",
        "maxColumns" -> providedSchema.fields.length.toString()
      )
    )

object MeasurementTypes extends Enumeration {
  type MeasurementType = Value

  val beforeRemediation = Value("Messung")
  val afterRemediation = Value("Messung nach der Sanierung")
}
