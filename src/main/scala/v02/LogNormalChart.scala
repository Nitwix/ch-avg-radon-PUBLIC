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

package v02

import data.RadonDatabase
import data.TownPopulations
import data_processing.Filtering
import main.CommandLineArgs
import main.{Symbols => s}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.jfree.chart.axis.NumberAxis
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYBarRenderer
import org.jfree.data.xy.XYDataset
import scalax.chart.XYChart
import scalax.chart.module.Imports.ChartTheme
import scalax.chart.module.XYChartFactories
import scalax.chart.module.XYDatasetConversions.ToIntervalXYDataset

import math.{abs, pow}
import org.jfree.chart.axis.NumberTickUnit
import org.jfree.data.statistics.SimpleHistogramDataset
import org.jfree.data.statistics.SimpleHistogramBin
import data_processing.CorrectedRnDb
import org.tinylog.scala.Logger
import scalax.chart.module.CategoryChartFactories
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.function.NormalDistributionFunction2D

import utils.MathUtils._

object LogConcentrationHistogram {

  /** Creates a new chart that represents numeric `x` (intervals) and `y` values
    * with a line.
    *
    * @param data
    *   $data
    * @param theme
    *   $theme
    * @param minX
    *   minimum x value shown on the bar chart
    * @param maxX
    *   maximum x value shown on the bar chart
    * @param binWidthExponent
    *   The bin width is computed as: binWidth = 2^binWidthExponent Typical The
    *   value *must be* comprised in {-10...2}
    *
    * @usecase
    *   def apply(data: IntervalXYDataset): XYChart = ???
    * @inheritdoc
    */
  def apply(
      data: Array[Double],
      minX: Double,
      maxX: Double,
      binWidthExponent: Int
  )(implicit theme: ChartTheme = ChartTheme.Default): XYChart = {
    assert(
      -10 <= binWidthExponent && binWidthExponent <= 2,
      "binWidthExponent out of bounds {-10...2}"
    )
    val binWidth = pow(2, binWidthExponent)
    assert( // safety measure, but in principle it shouldn't be needed
      closeTo(log2(binWidth).toInt.toDouble, log2(binWidth)),
      f"Bin width: $binWidth is NOT a power of 2! Can cause computation imprecisions and thus bin overlap"
    )

    assert(maxX > minX, f"$maxX <= $minX, (maxX <= minX)")
    val nbBins = ((maxX - minX) / binWidth).toInt

    val dataset = new SimpleHistogramDataset(binWidth)
    // add bins to histogram
    for (x <- (0 until nbBins).map(_ * binWidth + minX)) {
      val bin = new SimpleHistogramBin(x, x + binWidth, true, false)
      // println(f"SimpleHistogramBin($x, ${x+binWidth}, true, false)")
      dataset.addBin(bin)
    }

    data.foreach(c => {
      try {
        dataset.addObservation(c)
      } catch {
        case r: RuntimeException => {
          Logger.debug(
            f"Couldn't add value $c to histogram: No bin can accept it.\nException: ${r.getMessage()}"
          )
        }
      }
    })

    val domainAxis = new NumberAxis("Log(radon concentration)")

    val rangeAxis = new NumberAxis("Occurences in bin")

    val renderer = new XYBarRenderer()

    val plot = new XYPlot(dataset, domainAxis, rangeAxis, renderer)

    XYChart(
      plot,
      title = "Histogram of log of radon concentrations (~log-normal distr.)",
      legend = true
    )
  }

}

object LogNormalChart {

  def log = MainV02.log

  val minX = -1.0
  val maxX = 10.0
  val graphXResolutionExp: Int = -3
  val chartResolution = (1920/2, 1080/2)

  def showHistogram(corrRnDb: CorrectedRnDb)(implicit
      spark: SparkSession,
      cliArgs: CommandLineArgs
  ): Unit = {
    import spark.implicits._

    val logConcentrations = corrRnDb.df
      .map((row: Row) => {
        log(row.getAs[Double](s.floorCorrectedConc))
      })
      .collect()

    val chart = LogConcentrationHistogram(logConcentrations, minX, maxX, graphXResolutionExp)

    chart.show(resolution = chartResolution)
  }

  def showLogNormal(
      logMean: MainV02.LogMean,
      logStdDev: MainV02.LogStdDev
  ): Unit = {
    val dataset = new DefaultCategoryDataset()
    val normalDistr = new NormalDistributionFunction2D(logMean.m, logStdDev.s)
    val rowKey = "y"
    val distBetweenValues = pow(2, graphXResolutionExp)
    val nbValues = ((maxX - minX) / distBetweenValues).toInt
    for(
      x <- (0 until nbValues).map(_ * distBetweenValues + minX)
    ){
      dataset.addValue(normalDistr.getValue(x), rowKey, x)
    }

    val chart = CategoryChartFactories.LineChart(dataset)
    chart.show(resolution = chartResolution)
  }
}
