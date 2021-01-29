/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentSkipListSet}

import swaydb.data.config.ActorConfig.QueueOrder

protected sealed trait ActorQueue[T] {
  def add(item: T): Boolean
  def poll(): T
  def peek(): T
  def clear(): Unit
  def size: Int
  def isEmpty: Boolean
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
          override def add(item: (T, Int)): Boolean =
            queue add item

          override def poll(): (T, Int) =
            queue.poll()

          override def peek(): (T, Int) =
            queue.peek()

          override def clear(): Unit =
            queue.clear()

          def size: Int =
            queue.size

          override def isEmpty: Boolean =
            queue.isEmpty
        }

      case ordered: QueueOrder.Ordered[T] =>
        new ActorQueue[(T, Int)] {
          val skipList: ConcurrentSkipListSet[(T, Int)] = new ConcurrentSkipListSet[(T, Int)](ordered.ordering.on[(T, Int)](_._1))

          override def add(item: (T, Int)): Boolean =
            skipList add item

          override def poll(): (T, Int) =
            skipList.pollFirst()

          override def peek(): (T, Int) =
            skipList.first()

          override def clear(): Unit =
            skipList.clear()

          def size: Int =
            skipList.size

          override def isEmpty: Boolean =
            skipList.isEmpty
        }
    }
}
