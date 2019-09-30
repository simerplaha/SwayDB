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

package swaydb

import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentSkipListSet}

import swaydb.data.config.ActorConfig.QueueOrder

protected sealed trait ActorQueue[T] {
  def add(item: T): Unit
  def poll(): T
  def peek(): T
  def clear(): Unit
  def size: Int
}

protected object ActorQueue {
  /**
   * synchronized around add and clear because after [[Actor.terminate]]
   * the actor should to queue more messages.
   */
  def apply[T](queueOrder: QueueOrder[T]): ActorQueue[(T, Int)] =
    queueOrder match {
      case QueueOrder.FIFO =>
        new ActorQueue[(T, Int)] {
          val queue = new ConcurrentLinkedQueue[(T, Int)]()
          override def add(item: (T, Int)): Unit =
            queue add item

          override def poll(): (T, Int) =
            queue.poll()

          override def peek(): (T, Int) =
            queue.peek()

          override def clear(): Unit =
            queue.clear()

          def size: Int =
            queue.size
        }

      case ordered: QueueOrder.Ordered[T] =>
        new ActorQueue[(T, Int)] {
          val skipList: ConcurrentSkipListSet[(T, Int)] = new ConcurrentSkipListSet[(T, Int)](ordered.ordering.on[(T, Int)](_._1))

          override def add(item: (T, Int)): Unit =
            skipList add item

          override def poll(): (T, Int) =
            skipList.pollFirst()

          override def peek(): (T, Int) =
            skipList.first()

          override def clear(): Unit =
            skipList.clear()

          def size: Int =
            skipList.size
        }
    }
}
