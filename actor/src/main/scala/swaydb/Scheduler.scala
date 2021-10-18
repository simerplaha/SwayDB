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
