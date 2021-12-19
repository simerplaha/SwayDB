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

package swaydb.actor

import com.typesafe.scalalogging.LazyLogging
import swaydb.{ActorRef, DefActor, Glass, Scheduler}
import swaydb.core.cache.{Cache, CacheUnsafe}

import scala.collection.mutable.ListBuffer

object ActorTestSweeper extends LazyLogging {

  def apply[T](code: ActorTestSweeper => T): T = {
    val sweeper = new ActorTestSweeper {}
    val result = code(sweeper)
    sweeper.terminateAllActors()
    result
  }

  implicit class SchedulerSweeperImplicits(scheduler: Scheduler) {
    def sweep()(implicit sweeper: ActorTestSweeper): Scheduler =
      sweeper sweepScheduler scheduler
  }

  implicit class ActorsSweeperImplicits[T, S](actor: ActorRef[T, S]) {
    def sweep()(implicit sweeper: ActorTestSweeper): ActorRef[T, S] =
      sweeper sweepActor actor
  }

  implicit class ActorWiresSweeperImplicits[T](actor: DefActor[T]) {
    def sweep()(implicit sweeper: ActorTestSweeper): DefActor[T] =
      sweeper sweepActor actor
  }

}

trait ActorTestSweeper extends LazyLogging {

  private val schedulers: ListBuffer[CacheUnsafe[Unit, Scheduler]] = ListBuffer(Cache.unsafe[Unit, Scheduler](true, true, None)((_, _) => Scheduler()))
  private val actors: ListBuffer[ActorRef[_, _]] = ListBuffer.empty
  private val defActors: ListBuffer[DefActor[_]] = ListBuffer.empty

  implicit lazy val scheduler: Scheduler = schedulers.head.getOrFetch(())

  protected def removeReplaceCache[I, O](sweepers: ListBuffer[CacheUnsafe[I, O]], replace: O): O = {
    if (sweepers.lastOption.exists(_.get().isEmpty))
      sweepers.remove(0)

    val cache = Cache.unsafe[I, O](true, true, Some(replace))((_, _) => replace)
    sweepers += cache
    replace
  }

  def sweepScheduler(schedule: Scheduler): Scheduler =
    removeReplaceCache(schedulers, schedule)

  def sweepActor[T, S](actor: ActorRef[T, S]): ActorRef[T, S] = {
    actors += actor
    actor
  }

  def sweepActor[T](actor: DefActor[T]): DefActor[T] = {
    defActors += actor
    actor
  }

  /**
   * Terminates all sweepers immediately.
   */
  def terminateAllActors(): Unit = {
    logger.info(s"Terminating ${classOf[ActorTestSweeper].getSimpleName}")

    actors.foreach(_.terminateAndClear[Glass]())
    defActors.foreach(_.terminateAndClear[Glass]())

    schedulers.foreach(_.get().foreach(_.terminate()))
  }

}
