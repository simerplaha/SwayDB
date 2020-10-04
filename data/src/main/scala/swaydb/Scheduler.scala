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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb

import java.util.{Timer, TimerTask}

import scala.concurrent._
import scala.concurrent.duration.FiniteDuration

object Scheduler {

  /**
   * Creates a Scheduler.
   *
   * @param name     the name of the associated thread
   * @param isDaemon true if the associated thread should run as a daemon
   */
  def apply(name: Option[String] = None,
            isDaemon: Boolean = true)(implicit ec: ExecutionContext): Scheduler =
    name match {
      case Some(name) =>
        new Scheduler(new Timer(name, isDaemon))

      case None =>
        new Scheduler(new Timer(isDaemon))
    }
}

class Scheduler private(timer: Timer)(implicit val ec: ExecutionContext) {

  def apply[T](delayFor: FiniteDuration)(block: => Future[T]): Future[T] = {
    val promise = Promise[T]()
    val task =
      new TimerTask {
        def run(): Unit = {
          ec.execute(
            new Runnable {
              override def run(): Unit =
                promise.completeWith(block)
            }
          )
        }
      }
    timer.schedule(task, delayFor.toMillis max 0)
    promise.future
  }

  def future[T](delayFor: FiniteDuration)(block: => T): Future[T] =
    apply(delayFor)(Future(block))

  def futureFromIO[T](delayFor: FiniteDuration)(block: => IO[swaydb.Error.Segment, T]): Future[T] =
    apply(delayFor)(Future(block).flatMap(_.toFuture))

  def task(delayFor: FiniteDuration)(block: => Unit): TimerTask = {
    val task =
      new TimerTask {
        def run() =
          block
      }
    timer.schedule(task, delayFor.toMillis max 0)
    task
  }

  def terminate(): Unit =
    timer.cancel()
}
