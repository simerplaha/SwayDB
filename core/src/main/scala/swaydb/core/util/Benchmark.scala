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

import java.util.function.Supplier

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.util.Maths

object Benchmark extends LazyLogging {

  @FunctionalInterface
  trait Code {
    def run(): Unit
  }

  def doPrint(message: String,
              useLazyLogging: Boolean,
              newLine: Boolean) =
    if (useLazyLogging)
      logger.info(message)
    else if (newLine)
      println(message)
    else
      print(message)

  private def run[R](message: String, inlinePrint: Boolean, useLazyLogging: Boolean)(benchmarkThis: => R): (R, BigDecimal) = {
    if (!useLazyLogging) //don't need header Benchmarking log when using lazyLogging. LazyLogging is generally for user's viewing only.
      if (inlinePrint)
        doPrint(message = s"Benchmarking: $message: ", useLazyLogging = useLazyLogging, newLine = false)
      else
        doPrint(message = s"Benchmarking: $message", useLazyLogging = useLazyLogging, newLine = true)

    val startTime = System.nanoTime()
    val result = benchmarkThis
    val endTime = System.nanoTime()
    val timeTaken = (endTime - startTime) / 1000000000.0: Double
    val timeTakenRounded = Maths.round(timeTaken)

    if (inlinePrint)
      doPrint(message = timeTakenRounded + s" seconds - $message.", useLazyLogging = useLazyLogging, newLine = false)
    else
      doPrint(message = timeTakenRounded + s" seconds - $message.", useLazyLogging = useLazyLogging, newLine = true)

    if (!useLazyLogging)
      println

    (result, timeTakenRounded)
  }

  def apply[R](message: String, inlinePrint: Boolean = false, useLazyLogging: Boolean = false)(benchmarkThis: => R): R =
    run(
      message = message,
      inlinePrint = inlinePrint,
      useLazyLogging = useLazyLogging
    )(benchmarkThis)._1

  def time(message: String, inlinePrint: Boolean = false, useLazyLogging: Boolean = false)(benchmarkThis: => Unit): BigDecimal =
    run(
      message = message,
      inlinePrint = inlinePrint,
      useLazyLogging = useLazyLogging
    )(benchmarkThis)._2

  def java[R](message: String, supplier: Supplier[R]): R =
    apply(message)(supplier.get())

  def java(message: String, code: Code): Unit =
    apply(message)(code.run())

  def java[R](supplier: Supplier[R]): R =
    apply("")(supplier.get())

  def java(code: Code): Unit =
    apply("")(code.run())
}
