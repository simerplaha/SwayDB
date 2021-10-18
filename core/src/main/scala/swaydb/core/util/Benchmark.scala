/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.util

import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag.Implicits._
import swaydb.utils.Maths
import swaydb.{Bag, Glass}

import java.lang.management.ManagementFactory
import java.util.function.Supplier
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

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

  private def run[R, BAG[_]](message: String, inlinePrint: Boolean, useLazyLogging: Boolean)(benchmarkThis: => BAG[R])(implicit bag: Bag[BAG]): BAG[(R, BigDecimal)] = {
//    if (!useLazyLogging) //don't need header Benchmarking log when using lazyLogging. LazyLogging is generally for user's viewing only.
//      if (inlinePrint)
//        doPrint(message = s"Benchmarking: ${if (message.isEmpty) "" else s"$message: "}", useLazyLogging = useLazyLogging, newLine = false)
//      else
//        doPrint(message = s"Benchmarking: $message", useLazyLogging = useLazyLogging, newLine = true)

    val collectionTimeBefore = ManagementFactory.getGarbageCollectorMXBeans.asScala.foldLeft(0L)(_ + _.getCollectionTime)

    val startTime = System.nanoTime()

    benchmarkThis flatMap {
      result =>
        val endTime = System.nanoTime()
        val timeTaken = (endTime - startTime) / 1000000000.0: Double

        val collectionTimeAfter = ManagementFactory.getGarbageCollectorMXBeans.asScala.foldLeft(0L)(_ + _.getCollectionTime)

        val gcTimeTaken = (collectionTimeAfter - collectionTimeBefore) / 1000.0

        val timeWithoutGCRounded = Maths.round(timeTaken - gcTimeTaken)

        val messageToLog = s"${if (message.isEmpty) "" else s"$message - "}$timeWithoutGCRounded seconds. GC: ${Maths.round(gcTimeTaken)}. Total: ${Maths.round(timeTaken)}"

        if (inlinePrint)
          doPrint(message = messageToLog, useLazyLogging = useLazyLogging, newLine = false)
        else
          doPrint(message = messageToLog, useLazyLogging = useLazyLogging, newLine = true)

        if (!useLazyLogging)
          println

        bag.success(result, timeWithoutGCRounded)
    }
  }

  def apply[R](message: String, inlinePrint: Boolean = false, useLazyLogging: Boolean = false)(benchmarkThis: => R): R =
    run[R, Glass](
      message = message,
      inlinePrint = inlinePrint,
      useLazyLogging = useLazyLogging
    )(benchmarkThis)._1

  def future[R](message: String, inlinePrint: Boolean = false, useLazyLogging: Boolean = false)(benchmarkThis: => Future[R])(implicit ec: ExecutionContext): Future[R] =
    run(
      message = message,
      inlinePrint = inlinePrint,
      useLazyLogging = useLazyLogging
    )(benchmarkThis).map(_._1)

  def time(message: String, inlinePrint: Boolean = false, useLazyLogging: Boolean = false)(benchmarkThis: => Unit): BigDecimal =
    run[Unit, Glass](
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
