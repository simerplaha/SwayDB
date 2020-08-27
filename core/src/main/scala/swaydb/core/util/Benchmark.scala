/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.util

import com.typesafe.scalalogging.LazyLogging


object Benchmark extends LazyLogging {
  def doPrint(message: String,
              useLazyLogging: Boolean) =
    if (useLazyLogging)
      logger.info(message)
    else
      print(message)

  private def run[R](message: String, inlinePrint: Boolean, useLazyLogging: Boolean)(benchmarkThis: => R): (R, Double) = {
    if (inlinePrint)
      doPrint(s"Benchmarking: $message: ", useLazyLogging)
    else
      doPrint(s"Benchmarking: $message\n", useLazyLogging)
    val startTime = System.nanoTime()
    val result = benchmarkThis
    val endTime = System.nanoTime()
    val timeTaken = (endTime - startTime) / 1000000000.0: Double
    if (inlinePrint)
      doPrint(timeTaken + s" seconds - $message.", useLazyLogging)
    else
      doPrint(timeTaken + s" seconds - $message.\n", useLazyLogging)

    if (!useLazyLogging)
      println

    (result, timeTaken)
  }

  def apply[R](message: String, inlinePrint: Boolean = false, useLazyLogging: Boolean = false)(benchmarkThis: => R): R =
    run(
      message = message,
      inlinePrint = inlinePrint,
      useLazyLogging = useLazyLogging
    )(benchmarkThis)._1

  def time(message: String, inlinePrint: Boolean = false, useLazyLogging: Boolean = false)(benchmarkThis: => Unit): Double =
    run(
      message = message,
      inlinePrint = inlinePrint,
      useLazyLogging = useLazyLogging
    )(benchmarkThis)._2
}
