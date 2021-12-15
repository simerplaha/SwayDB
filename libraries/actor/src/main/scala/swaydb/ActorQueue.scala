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

import swaydb.ActorConfig.QueueOrder

import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentSkipListSet}

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
