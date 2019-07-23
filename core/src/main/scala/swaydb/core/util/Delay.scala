/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

import java.util.{Timer, TimerTask}

import swaydb.IO

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

object Delay {

  val futureNone = Future.successful(None)

  val futureUnit = Future.successful(())

  private val timer = new Timer(true)

  private def runWithDelay[T](delayFor: FiniteDuration)(block: => Future[T])(implicit ctx: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    val task =
      new TimerTask {
        def run() {
          ctx.execute(
            new Runnable {
              override def run(): Unit =
                promise.completeWith(block)
            }
          )
        }
      }
    timer.schedule(task, delayFor.toMillis)
    promise.future
  }

  private def createTask(delayFor: FiniteDuration)(block: => Unit): TimerTask = {
    val task =
      new TimerTask {
        def run() =
          block
      }
    timer.schedule(task, delayFor.toMillis)
    task
  }

  def apply[T](delayFor: FiniteDuration)(block: => Future[T])(implicit ctx: ExecutionContext): Future[T] =
    runWithDelay(delayFor)(block)

  def future[T](delayFor: FiniteDuration)(block: => T)(implicit ctx: ExecutionContext): Future[T] =
    apply(delayFor)(Future(block))

  def futureFromIO[T](delayFor: FiniteDuration)(block: => IO[IO.Error, T])(implicit ctx: ExecutionContext): Future[T] =
    apply(delayFor)(Future(block).flatMap(_.toFuture))

  def task(delayFor: FiniteDuration)(block: => Unit): TimerTask =
    createTask(delayFor)(block)

  def cancelTimer(): Unit =
    timer.cancel()
}
