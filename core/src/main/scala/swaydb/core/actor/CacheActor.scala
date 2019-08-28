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

package swaydb.core.actor

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ErrorHandler
import swaydb.IO
import swaydb.data.config.ActorConfig

import scala.collection.mutable

private class State[T](var size: Long,
                       val queue: mutable.Queue[(T, Long)])

private[core] object CacheActor {

  def apply[T](maxWeight: Long,
               actorConfig: ActorConfig,
               weigher: T => Long)(onEvict: T => Unit): CacheActor[T] =
    new CacheActor(
      maxWeight = maxWeight,
      onEvict = onEvict,
      actorConfig = actorConfig,
      weigher = weigher
    )
}

/**
 * Evicts items from Queue (if the queue exceeds the limit) at regular intervals of delays.
 *
 * Next delay is adjusted based on the current overflow. If elements value added to the queue more
 * frequently than the eviction occurs then the next eviction delay is adjusted to run more often.
 */
private[core] class CacheActor[T](maxWeight: Long,
                                  onEvict: T => Unit,
                                  actorConfig: ActorConfig,
                                  weigher: T => Long) extends LazyLogging {

  //  logger.info(s"${this.getClass.getSimpleName} started with limit: {}, defaultDelay: {}", limit, defaultDelay)

  private val actor: ActorRef[T] =
    Actor.fromConfig[T, State[T]](actorConfig, new State(0, mutable.Queue())) {
      case (item, self) =>
        //        println("Item: " + item)
        val weight = weigher(item)
        if (weight != 0) {
          self.state.queue += ((item, weight))
          self.state.size += weight

          //        println("---------------- Total size: " + self.state.size)
          while (self.state.size > maxWeight) {
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
    actor ! item

  def hasMessages =
    actor.hasMessages

  def terminate() =
    actor.terminate()
}