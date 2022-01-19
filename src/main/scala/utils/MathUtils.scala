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

package utils

import math.{log, abs}

object MathUtils {
  def roundAt(p: Int)(n: Double): Double = {
    val s = math pow (10, p)
    (math round n * s) / s
  }

  /** Log base 2
    *
    * @param d
    * @return
    */
  def log2 = logBaseB(2.0)(_)

  /** Returns the log base b of x, where b is an arbitrary base for the log.
    *
    * @param b
    *   Base of the log
    * @param x
    *   Argument of the log
    * @return
    *   log_b(x)
    */
  def logBaseB(b: Double)(x: Double) = log(x) / log(b)

  /** Returns whether x and y are close to each other. This is only true if they
    * are at a distance smaller than Double.MinPositiveValue .
    *
    * @param x
    * @param y
    * @return
    *   true iff abs(x-y) < Double.MinPositiveValue
    */
  def closeTo(x: Double, y: Double): Boolean = {
    abs(x - y) < Double.MinPositiveValue
  }
}
