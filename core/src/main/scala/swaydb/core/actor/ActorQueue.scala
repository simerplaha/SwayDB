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

import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentSkipListSet}

import swaydb.data.config.ActorConfig.QueueOrder

protected sealed trait ActorQueue[T] {
  def add(item: T): Unit
  def poll(): T
  def clear(): Unit
}

protected object ActorQueue {
  def apply[T](queueOrder: QueueOrder[T]): ActorQueue[T] =
    queueOrder match {
      case QueueOrder.FIFO =>
        new ActorQueue[T] {
          val queue = new ConcurrentLinkedQueue[T]()
          override def add(item: T): Unit =
            queue add item

          override def poll(): T =
            queue.poll()

          override def clear(): Unit =
            queue.clear()
        }

      case ordered: QueueOrder.Ordered[T] =>
        new ActorQueue[T] {
          val skipList: ConcurrentSkipListSet[T] = new ConcurrentSkipListSet[T](ordered.ordering)

          override def add(item: T): Unit =
            skipList add item

          override def poll(): T =
            skipList.pollFirst()

          override def clear(): Unit =
            skipList.clear()
        }
    }
}
