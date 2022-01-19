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

package v02

import data.AverageFloor2020
import data.DataFrameWrapper
import data.RadonDatabase
import data.TownPopulations
import data_processing.CorrectedRnDb
import data_processing.Corrections
import data_processing.FilteredRnDb
import data_processing.Filtering
import main.CommandLineArgs
import main.Flags
import main.{Symbols => s}
import org.apache.spark.sql._
import org.jfree.chart.StandardChartTheme
import org.jfree.chart.renderer.category.BarRenderer
import org.tinylog.scala.Logger
import scalax.chart.module.Imports
import scalax.chart.module.XYChartFactories
import utils.MathUtils

import scala.math.sqrt

/** Contains static methods related to the computation of version 02 of the
  * average. The goal of this version of the computation of the average radon
  * concentration is to fit a log-normal distribution to the concentration
  * samples.
  * => https://en.wikipedia.org/wiki/Log-normal_distribution
  */
object MainV02 {

  val logBase: Double = 10

  def exp(exponent: Double): Double = {
    scala.math.pow(logBase, exponent)
    //scala.math.exp(exponent)
  }
  def log: Double => Double = {
    MathUtils.logBaseB(logBase)
    //scala.math.log(_)
  }

  /** Returns the average radon concentration in Bq/m^3, according to v02 of the
    * computation.
    *
    * @param spark
    *   SparkSession required for spark
    * @param cliArgs
    *   CommandLineArgs, as parsed from main()
    * @return
    *   Average radon concentration in Bq/m^3
    */
  def computeV02(implicit
      spark: SparkSession,
      cliArgs: CommandLineArgs
  ): Double = {
    val rnDb = RadonDatabase()
    val filteredRnDb = Filtering.filterRnDb(rnDb, TownPopulations())
    val correctedRnDb =
      Corrections.correctForFloor(filteredRnDb, AverageFloor2020())
    val logCorrRnDb = logCorrectedRnDb(correctedRnDb)
    val (logMean, logStdDev) = computeLogNormalMaxLikelihood(logCorrRnDb)
    println(logMean, logStdDev)
    println(
      f"In original units: Mean = ${logMean.toOriginalUnits} Bq/m^3, StdDev = ${logStdDev.toOriginalUnits} Bq/m^3"
    )
    val perc1LogSigma =
      percentageWithin1LogSigmaOfMean(logCorrRnDb, logMean, logStdDev)
    Logger.info(f"Percentage within 1 log(sigma) of log(mean) = $perc1LogSigma")

    if (cliArgs.isFlagEnabled(Flags.ShowV02Charts)) {
      LogNormalChart.showHistogram(correctedRnDb)
      LogNormalChart.showLogNormal(logMean, logStdDev)
    }

    logMean.toOriginalUnits
  }

  /** Represents the logarithm of the mean of a distribution
    *
    * @param m
    *   Value of the mean
    */
  case class LogMean(m: Double) {

    /** Returns the mean in the original units */
    def toOriginalUnits: Double = exp(m)
  }

  /** Represents the logarithm of the standard deviation of a distribution
    *
    * @param s
    *   Value of the standard deviation
    */
  case class LogStdDev(s: Double) {

    /** Returns the standard deviation in the original units */
    def toOriginalUnits: Double = exp(s)
  }

  /** Returns the maximum likelihood estimator for the log-normal distribution
    * for the mean and the standard deviation of the concentrations.
    *
    * For more information on the computation of the estimators, see
    * https://en.wikipedia.org/wiki/Log-normal_distribution#Estimation_of_parameters
    *
    * @param correctedRnDb
    *   radon db, after corrections.
    * @param spark
    *   SparkSession
    * @param cliArgs
    *   CommandLineArgs
    * @return
    *   Pair of the log of the mean and standard deviation
    */
  def computeLogNormalMaxLikelihood(
      logCorrRnDb: LogCorrectedRnDb
  )(implicit
      spark: SparkSession,
      cliArgs: CommandLineArgs
  ): (LogMean, LogStdDev) = {

    val logMuHat = logNormalMaxLikelihoodMean(logCorrRnDb)
    val logSigmaHat = logNormalMaxLikelihoodStdDev(logCorrRnDb, logMuHat)

    (logMuHat, logSigmaHat)
  }

  val logConcSymbol = "LOG_RN_KONZENTRATION_BQ_M3"

  /** Returns the log of maximum likelihood estimator for the mean of the
    * concentrations of the given dataset.
    *
    * @param logCorrRnDb
    *   Dataset containing the log of the concentrations
    * @param spark
    *   SparkSession
    * @return
    *   log of the max likelihood estimator for the mean
    */
  def logNormalMaxLikelihoodMean(
      logCorrRnDb: LogCorrectedRnDb
  )(implicit spark: SparkSession): LogMean = {
    import spark.implicits._
    val m = logCorrRnDb.df
      .agg(logConcSymbol -> "avg")
      .collect()
      .head
      .getAs[Double](0)
    LogMean(m)
  }

  /** Returns the log of maximum likelihood estimator for the mean of the
    * concentrations of the given dataset.
    *
    * @param logCorrRnDb
    *   Dataset containing the log of the concentrations
    * @param logMean
    *   Log of the maximum likelihood estimator for the mean
    * @param spark
    *   SparkSession
    * @return
    *   log of the max likelihood estimator for the mean
    */
  def logNormalMaxLikelihoodStdDev(
      logCorrRnDb: LogCorrectedRnDb,
      logMean: LogMean
  )(implicit
      spark: SparkSession
  ): LogStdDev = {
    import spark.implicits._
    val diffFromMeanSquared = "DIFF_FROM_MEAN_SQUARED"
    val sigmaHatSquared = logCorrRnDb.df
      .map[Double]((row: Row) => {
        val logxk = row.getAs[Double](logConcSymbol)
        val dev = logxk - logMean.m
        dev * dev
      })
      .toDF(diffFromMeanSquared)
      .agg(diffFromMeanSquared -> "avg")
      .collect()
      .head
      .getAs[Double](0)
    LogStdDev(sqrt(sigmaHatSquared))
  }

  /** Wraps a DataFrame containing ONLY the log of the concentrations (that have
    * been previously filtered and corrected)
    *
    * @param df
    *   Wrapped DataFrame
    */
  case class LogCorrectedRnDb(df: DataFrame) extends DataFrameWrapper

  /** Returns the log of the values of radon concentration of the given
    * CorrectedRnDb.
    *
    * Warning: this method keeps only the concentration values, and drops all
    * other information of the radon db.
    *
    * @param corrRnDb
    *   Contains the concentrations to be converted
    * @param spark
    *   SparkSession
    * @return
    *   Log of concentrations
    */
  def logCorrectedRnDb(
      corrRnDb: CorrectedRnDb
  )(implicit spark: SparkSession): LogCorrectedRnDb = {
    import spark.implicits._
    val lgRnDb = corrRnDb.df
      .select(s.floorCorrectedConc)
      .map[Double]((row: Row) => {
        val measurement = row.getAs[Double](s.floorCorrectedConc)
        log(measurement)
      })
      .toDF(logConcSymbol)
    LogCorrectedRnDb(lgRnDb)
  }

  /** Models a percentage value.
    *
    * @param p
    *   Percentage, as a Double. Must be between 0.0 and 1.0, both inclusive.
    */
  case class Percentage(p: Double) {
    assert(
      0.0 <= p && p <= 1.0,
      f"Value $p is not a valid percentage (not in [0.0, 1.0])"
    )
    override def toString: String = f"${100 * MathUtils.roundAt(2)(p)}%%"
  }

  /** Returns the percentage of concentration values that are within 1 log of
    * standard deviation of the log of the mean. This value should be >= 68% if
    * the distribution is normal.
    *
    * @param lgCorrRnDb
    *   Log of the concentrations
    * @param logMean
    *   Log of the mean of concentrations
    * @param logStdDev
    *   Log of the standard deviation of concentrations
    * @return
    *   Percentage of log of concentration values that fall within 1 sigma of
    *   the log of the mean.
    */
  def percentageWithin1LogSigmaOfMean(
      lgCorrRnDb: LogCorrectedRnDb,
      logMean: LogMean,
      logStdDev: LogStdDev
  ): Percentage = {
    val lower = logMean.m - logStdDev.s
    val upper = logMean.m + logStdDev.s
    val within1Sigma = lgCorrRnDb.df
      .filter((row: Row) => {
        val logxk = row.getAs[Double](logConcSymbol)
        lower <= logxk && logxk <= upper
      })
      .count()

    Percentage(within1Sigma.toDouble / lgCorrRnDb.df.count())
  }
}
