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

package swaydb.core.queue

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.core.actor.{Actor, ActorRef}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

private class State[T](var size: Long,
                       val queue: mutable.Queue[(T, Long)])

private[core] object LimitQueue {

  def apply[T](limit: Long,
               delay: FiniteDuration,
               weigher: T => Long)(onEvict: T => Unit)(implicit ec: ExecutionContext): LimitQueue[T] =
    new LimitQueue(limit, onEvict, delay, weigher)
}

/**
 * Evicts items from Queue (if the queue exceeds the limit) at regular intervals of delays.
 *
 * Next delay is adjusted based on the current overflow. If elements value added to the queue more
 * frequently than the eviction occurs then the next eviction delay is adjusted to run more often.
 *
 * @param limit        max size of the queue
 * @param onEvict      function on trigger on evicted item
 * @param defaultDelay interval delays to run overflow checks which is adjust
 *                     during runtime based on the frequency of items being added and the size of overflow.
 */
private[core] class LimitQueue[T](limit: Long,
                                  onEvict: T => Unit,
                                  defaultDelay: FiniteDuration,
                                  weigher: T => Long)(implicit ec: ExecutionContext) extends LazyLogging {

  //  logger.info(s"${this.getClass.getSimpleName} started with limit: {}, defaultDelay: {}", limit, defaultDelay)

  private val actor: ActorRef[T] =
    Actor.timerLoop[T, State[T]](new State(0, mutable.Queue()), defaultDelay) {
      case (item, self) =>
        //        println("Item: " + item)
        val weight = weigher(item)
        if (weight != 0) {
          self.state.queue += ((item, weight))
          self.state.size += weight

          //        println("---------------- Total size: " + self.state.size)
          while (self.state.size > limit) {
            //          println("Running eviction. Limit: {}, currentSize: {}", limit, self.state.size)
            IO(self.state.queue.dequeue) foreach {
              case (evictedItem, evictedItemSize) =>
                //println(s"evictedItem: $evictedItem, evictedItemSize: $evictedItemSize")
                self.state.size -= evictedItemSize
                try
                  onEvict(evictedItem)
                catch {
                  case ex: Exception =>
                    logger.error("Eviction error", ex)
                }
            }
          }
        }
    }

  def !(item: T): Unit =
    actor.submit(item)

  def hasMessages =
    actor.hasMessages

  def terminate() =
    actor.terminate()
}