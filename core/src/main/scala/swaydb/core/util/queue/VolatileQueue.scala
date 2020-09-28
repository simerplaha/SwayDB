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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.util.queue

private[core] object VolatileQueue {

  def apply[A >: Null](): VolatileQueue[A] =
    apply[A](Node.Empty)

  def apply[A >: Null](value: A): VolatileQueue[A] =
    apply[A](new Node.Value(value, Node.Empty))

  private def apply[A >: Null](head: Node[A]): VolatileQueue[A] =
    head match {
      case Node.Empty =>
        new VolatileQueue(Node.Empty, Node.Empty)

      case value: Node.Value[A] =>
        new VolatileQueue(value, value)
    }
}

private[core] class VolatileQueue[A >: Null](@volatile var head: Node[A],
                                             @volatile private var last: Node[A]) extends Walker[A] { self =>

  def addHead(value: A): VolatileQueue[A] =
    this.synchronized {
      val next = new Node.Value[A](value, Node.Empty)

      head match {
        case Node.Empty =>
          head = next
          last = next

        case previousNext: Node.Value[A] =>
          head = next
          next.next = previousNext
      }

      self
    }

  def addLast(value: A): VolatileQueue[A] =
    this.synchronized {
      val newNext = new Node.Value[A](value, Node.Empty)

      self.head match {
        case Node.Empty =>
          head = newNext
          last = newNext

        case _: Node.Value[A] =>
          self.last match {
            case Node.Empty =>
              throw new Exception("If head is non-empty, last cannot be empty")

            case last: Node.Value[A] =>
              last.next match {
                case Node.Empty =>
                  last.next = newNext
                  self.last = newNext

                case _: Node.Value[A] =>
                  throw new Exception("Last's next was non-empty")
              }
          }
      }

      self
    }

  def pollHeadOrNull(): A =
    this.synchronized {
      self.head match {
        case Node.Empty =>
          null

        case value: Node.Value[A] =>
          self.head = value.next
          value.value
      }
    }

  def headOption(): Option[A] =
    Option(headOrNull())

  def headOrNull(): A =
    head match {
      case Node.Empty =>
        null

      case value: Node.Value[A] =>
        value.value
    }
}
