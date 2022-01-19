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

/** Enumerates the available flags that the program can receive from the command
  * line.
  */
object Flags extends Enumeration {
  type Flag = Value

  /** Displays a help message and exits */
  val Help = Value("help")

  /** Skips certain optional portions of code */
  val RunFast = Value("run-fast")

  /** Reproduces the computation of the average as it was done in 2004 with 2004
    * data
    */
  val Reproduce2004Computation = Value("repr-2004-comp")

  /** Launches the computation with the 2004 method */
  val V01 = Value("v01")

  /** Launches the computation that fits a log-normal distribution */
  val V02 = Value("v02")

  /** Displays charts relating to v02 (only works when --v02 enabled) */
  val ShowV02Charts = Value("show-v02-charts")

  /** Displays the GPLv3 warranty */
  val Warranty = Value("warranty")

  /** Displays the GPLv3 copyright */
  val Copyright = Value("copyright")
}

/** Enumerates possible parsing errors */
object ParsingErrors extends Enumeration {
  type ParsingError = Value

  val UnknownFlag = Value("Unknown flag provided to the program")
}

import Flags.Flag

/** Provides static methods related to command line arguments.
  */
object CommandLineArgs {
  val flagPrefix = "--"

  /** Returns the parsed command line arguments (wrapped in a [[scala.Option]]).
    * When the return value is None, a parsing error occured.
    *
    * @param args
    *   Command line arguments, exactly as received in main()
    * @return
    *   Parsed command line arguments
    */
  def parseArgs(args: Array[String]): Option[CommandLineArgs] = {
    import ParsingErrors.ParsingError
    var parsingErrors: Set[ParsingError] = Set()
    args.foreach(arg => {
      val argWithoutPrefix = arg.drop(flagPrefix.length())
      val unknownFlag =
        Flags.values.forall(flag => flag.toString() != argWithoutPrefix)
      if (unknownFlag) {
        println(f"WARNING: UNKNOWN FLAG $arg")
        parsingErrors += ParsingErrors.UnknownFlag
      }
    })

    val flagsMap = Flags.values
      .map(flag => {
        val flagEnabled = args.contains(f"--${flag.toString()}")
        (flag, flagEnabled)
      })
      .toMap[Flag, Boolean]

    if (parsingErrors.isEmpty) {
      Some(CommandLineArgs(flagsMap))
    } else {
      println(helpMessage)
      None
    }
  }

  lazy val helpMessage: String = {
    val out = new StringBuilder
    out ++= "The following flags are available:\n"
    Flags.values.foreach(out ++= "\t--" + _.toString() + '\n')
    out.mkString
  }
}

/** Provides command line arguments and related utility methods.
  *
  * @param flagsMap
  *   Maps flags to a boolean. A `value` for a flag indicates that it is
  *   enabled.
  */
final case class CommandLineArgs(flagsMap: Map[Flag, Boolean]) {

  /** Returns true if the given flag is enabled.
    *
    * @param flag
    *   Flag for which we want to know if it is enabled or not.
    */
  def isFlagEnabled(flag: Flag): Boolean = {
    flagsMap.get(flag) match {
      case None        => false
      case Some(value) => value
    }
  }

  /** Returns true iff all flags are enabled. */
  def areAllFlagsEnabled(flags: Flag*): Boolean = flags.forall(isFlagEnabled(_))

  /** Returns true iff all flags are disabled. */
  def areAllFlagsDisabled(flags: Flag*): Boolean =
    flags.forall(!isFlagEnabled(_))

  /** Returns true if *any* of the given flags is enabled */
  def isAnyFlagEnabled(flags: Flag*): Boolean = flags.exists(isFlagEnabled(_))
}
