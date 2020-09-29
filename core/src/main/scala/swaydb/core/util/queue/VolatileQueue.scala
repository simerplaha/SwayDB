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

  @inline def apply[A >: Null](): VolatileQueue[A] =
    apply[A](Node.Empty)

  @inline def apply[A >: Null](value: A): VolatileQueue[A] =
    apply[A](new Node.Value(value, Node.Empty, Node.Empty))

  @inline def apply[A >: Null](value: A*): VolatileQueue[A] =
    apply[A](value)

  def apply[A >: Null](value: Iterable[A]): VolatileQueue[A] = {
    val queue = VolatileQueue[A]()
    value.foreach(queue.addHead)
    queue
  }

  private def apply[A >: Null](head: Node[A]): VolatileQueue[A] =
    head match {
      case Node.Empty =>
        new VolatileQueue(_head = Node.Empty, _last = Node.Empty, _size = 0)

      case value: Node.Value[A] =>
        new VolatileQueue(_head = value, _last = value, _size = 1)
    }
}

/**
 * VolatileQueue which is concurrent for reads only. For [[swaydb.core.level.zero.LevelZero]]
 * we do not need concurrent writes as all writes a sequential.
 *
 * The reason to use this over [[java.util.concurrent.ConcurrentLinkedDeque]] is to improve
 * iteration performance in [[swaydb.core.level.zero.LevelZero]] with [[Walker]] and [[iterator]]
 * should reduce GC workload when performing reads.
 */
private[core] class VolatileQueue[A >: Null](@volatile private var _head: Node[A],
                                             @volatile private var _last: Node[A],
                                             @volatile private var _size: Int) extends Walker[A] { self =>

  private[queue] def head = _head

  def size: Int = _size

  def addHead(value: A): VolatileQueue[A] =
    this.synchronized {

      _head match {
        case Node.Empty =>
          val newHead = new Node.Value[A](value, Node.Empty, Node.Empty)

          _head = newHead
          _last = newHead

        case oldHead: Node.Value[A] =>
          val newHead = new Node.Value[A](value = value, previous = Node.Empty, next = oldHead)

          oldHead.previous = newHead
          _head = newHead
      }

      _size += 1

      self
    }

  def addLast(value: A): VolatileQueue[A] =
    this.synchronized {
      if (self._head.isEmpty) {
        //sizes are used to create Slices so update this first
        _size += 1

        val newLast = new Node.Value[A](value, Node.Empty, Node.Empty)

        _head = newLast
        _last = newLast
      } else {
        self._last match {
          case Node.Empty =>
            throw new Exception("If head is non-empty, last cannot be empty")

          case last: Node.Value[A] =>
            last.next match {
              case Node.Empty =>
                //sizes are used to create Slices so update this first
                _size += 1

                val newLast = new Node.Value[A](value = value, previous = last, next = Node.Empty)

                last.next = newLast
                self._last = newLast

              case _: Node.Value[A] =>
                throw new Exception("Last's next was non-empty")
            }
        }
      }

      self
    }

  def removeLast(expectedLast: A): Unit =
    this.synchronized {
      if (_last.isEmpty)
        throw new Exception("Last is empty")
      else if (_last.value != expectedLast)
        throw new Exception(s"Invalid remove. ${_last.value} != $expectedLast")
      else
        _last.previous match {
          case Node.Empty =>
            //this was the head entry
            assert(size == 1)
            //sizes are used to create Slices so update this first
            _size -= 1
            _last = Node.Empty
            _head = Node.Empty

          case previous: Node.Value[A] =>
            //unlink
            _size -= 1
            previous.next = Node.Empty
            _last = previous
        }
    }

  def replaceLast(expectedLast: A, replaceWith: A): Unit =
    this.synchronized {
      if (_last.isEmpty)
        throw new Exception("Last is empty")
      else if (_last.value != expectedLast)
        throw new Exception(s"Invalid remove. ${_last.value} != $expectedLast")
      else
        _last.previous match {
          case Node.Empty =>
            //this was the head entry
            assert(size == 1)
            val newLast = new Node.Value[A](replaceWith, Node.Empty, Node.Empty)
            _last = newLast
            _head = newLast

          case previous: Node.Value[A] =>
            val newLast = new Node.Value[A](replaceWith, previous, Node.Empty)
            previous.next = newLast
            _last = newLast
        }
    }

  def headOption(): Option[A] =
    Option(headOrNull())

  override def dropHead(): Walker[A] =
    if (_head.isEmpty || _head.next.isEmpty)
      Walker.empty
    else
      new VolatileQueue[A](_head.next, _head.next.next, self._size - 1)

  def lastOrNull(): A =
    if (_last.isEmpty) {
      assert(_head.isEmpty)
      null
    } else {
      assert(!_head.isEmpty)
      _last.value
    }

  def headOrNull(): A =
    if (_head.isEmpty)
      null
    else
      _head.value

  def walker: Walker[A] =
    self

  /**
   * [[iterator]] is less expensive this [[Walker]].
   * [[Walker]] should be used where lazy iterations are required
   * are required like searching levels.
   */
  def iterator: Iterator[A] =
    new Iterator[A] {
      var node: Node[A] = self._head
      var value: A = _

      override def hasNext: Boolean =
        if (node.isEmpty) {
          false
        } else {
          value = node.value
          node = node.next
          true
        }

      override def next(): A =
        value
    }
}
