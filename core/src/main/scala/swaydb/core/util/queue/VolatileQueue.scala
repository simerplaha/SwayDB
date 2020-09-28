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

  def apply[A >: Null](value: A*): VolatileQueue[A] =
    apply[A](value)

  def apply[A >: Null](value: Iterable[A]): VolatileQueue[A] = {
    val queue = VolatileQueue[A]()
    value.foreach(queue.addHead)
    queue
  }

  private def apply[A >: Null](head: Node[A]): VolatileQueue[A] =
    head match {
      case Node.Empty =>
        new VolatileQueue(head = Node.Empty, last = Node.Empty, size = 0)

      case value: Node.Value[A] =>
        new VolatileQueue(head = value, last = value, size = 1)
    }
}

private[core] class VolatileQueue[A >: Null](@volatile var head: Node[A],
                                             @volatile private var last: Node[A],
                                             @volatile var size: Int) extends Walker[A] { self =>


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

      size += 1

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

      size += 1

      self
    }

  def pollHeadOrNull(): A =
    this.synchronized {
      self.head match {
        case Node.Empty =>
          null

        case value: Node.Value[A] =>
          self.head = value.next
          size -= 1
          value.value
      }
    }

  def headOption(): Option[A] =
    Option(headOrNull())

  override def dropHead(): VolatileQueue[A] =
    head match {
      case Node.Empty =>
        VolatileQueue()

      case head: Node.Value[A] =>
        head.next match {
          case Node.Empty =>
            new VolatileQueue[A](Node.Empty, Node.Empty, 0)

          case node: Node.Value[A] =>
            new VolatileQueue[A](node, node.next, self.size - 1)
        }
    }

  def lastOrNull(): A =
    last match {
      case Node.Empty =>
        assert(head.isEmpty)
        null

      case node: Node.Value[A] =>
        assert(!head.isEmpty)
        node.value
    }

  def headOrNull(): A =
    head match {
      case Node.Empty =>
        null

      case node: Node.Value[A] =>
        node.value
    }

  def walker: Walker[A] =
    self

  /**
   * [[iterator]] is less expensive this [[Walker]].
   * [[Walker]] should be used where lazy iterations are required
   * are required like searching levels.
   */
  def iterator: Iterator[A] =
    new Iterator[A] {
      var headNode: Node[A] = self.head
      var headValue: A = _

      override def hasNext: Boolean = {
        headNode match {
          case Node.Empty =>
            false

          case node: Node.Value[A] =>
            headNode = node.next
            headValue = node.value
            true
        }
      }

      override def next(): A =
        headValue
    }
}
