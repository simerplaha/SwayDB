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
 */

package swaydb.core.util


object Benchmark {
  private def run[R](message: String, inlinePrint: Boolean = false)(benchmarkThis: => R): (R, Double) = {
    if (inlinePrint)
      print(s"Benchmarking: $message: ")
    else
      println(s"Benchmarking: $message")
    val startTime = System.nanoTime()
    val result = benchmarkThis
    val endTime = System.nanoTime()
    val timeTaken = ((endTime - startTime) / 1000000000.0: Double)
    if (inlinePrint)
      print(timeTaken + s" seconds - $message.")
    else
      println(timeTaken + s" seconds - $message.")
    println
    (result, timeTaken)
  }

  def apply[R](message: String, inlinePrint: Boolean = false)(benchmarkThis: => R): R =
    run(message, inlinePrint)(benchmarkThis)._1

  def time(message: String, inlinePrint: Boolean = false)(benchmarkThis: => Unit): Double =
    run(message, inlinePrint)(benchmarkThis)._2
}
