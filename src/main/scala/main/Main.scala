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

package main

import data._
import data_processing.Corrections
import data_processing.Filtering
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.tinylog.scala.Logger
import v01.ComputeAverageVolumetricActivity
import v01.MainV01
import v02.MainV02

/** Contains the entry point method `main`.
  */
object Main {

  /** Returns the [[org.apache.spark.sql.SparkSession]] for this program.
    *
    * Spark is configured to run locally on all available cores.
    *
    * @return
    *   the SparkSession
    */
  def configSparkSession: SparkSession = {
    val spark = SparkSession.builder
      .appName("ch-avg-radon-concentration")
      // https://spark.apache.org/docs/latest/configuration.html
      .config("spark.master", "local[*]")
      .config("spark.rpc.askTimeout", "200s")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") // disable all logging below ERROR
    spark
  }

  /** Prints a horizontal separator to the standard output */
  def printLogSeparator = println("=" * 80)

  /** Main entry point of the program. The available arguments are documented in
    * [[main.CommandLineArgs]]
    *
    * This method also prints license information
    *
    * @param args
    *   arguments of the program
    */
  def main(args: Array[String]): Unit = {
    println("""ch-avg-radon  Copyright (C) 2022  Niels Lachat
    This program comes with ABSOLUTELY NO WARRANTY; for details type `run --warranty'.
    This is free software, and you are welcome to redistribute it
    under certain conditions; type `run --copyright' for details.""")
    printLogSeparator

    import Flags._
    val maybeCliArgs = CommandLineArgs.parseArgs(args)
    if (maybeCliArgs == None) {
      println("Program ended early because of parsing errors")
      return
    }
    implicit val cliArgs = maybeCliArgs.get

    if(cliArgs.isFlagEnabled(Help)){
      println(CommandLineArgs.helpMessage)
    }

    if (cliArgs.isFlagEnabled(Warranty)) {
      println("""
                THERE IS NO WARRANTY FOR THE PROGRAM, TO THE EXTENT PERMITTED BY
        APPLICABLE LAW.  EXCEPT WHEN OTHERWISE STATED IN WRITING THE COPYRIGHT
        HOLDERS AND/OR OTHER PARTIES PROVIDE THE PROGRAM "AS IS" WITHOUT WARRANTY
        OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO,
        THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
        PURPOSE.  THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE PROGRAM
        IS WITH YOU.  SHOULD THE PROGRAM PROVE DEFECTIVE, YOU ASSUME THE COST OF
        ALL NECESSARY SERVICING, REPAIR OR CORRECTION.
      """)
      return
    }

    if (cliArgs.isFlagEnabled(Copyright)) {
      println("""
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
      """)
      return
    }

    if (cliArgs.areAllFlagsEnabled(V01, V02)) {
      println("Flags v01 and v02 are mutually exclusive")
      return
    }

    if (cliArgs.isAnyFlagEnabled(V01, V02)) {
      implicit val spark = configSparkSession

      // check if we got a flag that asks for a specific version of the computation
      if (cliArgs.isFlagEnabled(V01)) {
        printLogSeparator
        println(f"Computing v01")
        val v01Concentration = MainV01.computeV01
        printLogSeparator
        if (cliArgs.isFlagEnabled(Reproduce2004Computation)) {
          println(
            "REPRODUCTION OF 2004 COMPUTATION. ONLY POPULATION DATA IS FROM 2020"
          )
        }
        println(
          f"==> Average Rn volumetric concentration computed with 2004 method: " +
            f"${v01Concentration}"
        )
      } else if (cliArgs.isFlagEnabled(V02)) {
        printLogSeparator
        println(f"Computing v02")
        MainV02.computeV02
      }
      spark.stop()
    } else {
      printLogSeparator
      println("No computation requested (specify --v01 or --v02)")
    }
  }
}
