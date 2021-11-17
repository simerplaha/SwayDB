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

package swaydb.queue

import swaydb.utils.DropIterator

private[swaydb] object VolatileQueue {

  @inline def apply[A >: Null](): VolatileQueue[A] =
    apply[A](Node.Empty)

  @inline def apply[A >: Null](value: A*): VolatileQueue[A] =
    apply[A](value)

  def apply[A >: Null](value: Iterable[A]): VolatileQueue[A] = {
    val queue = VolatileQueue[A]()
    value.foreach(queue.addLast)
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
 * Concurrent for reads only. In [[swaydb.core.level.zero.LevelZero]]
 * we do not need concurrent writes as all writes a sequential.
 *
 * The reason to use this over [[java.util.concurrent.ConcurrentLinkedDeque]] is to improve
 * iteration performance in [[swaydb.core.level.zero.LevelZero]] with [[Walker]] and [[iterator]]
 * should reduce GC allocations when performing reads.
 */
private[swaydb] class VolatileQueue[A >: Null](@volatile private var _head: Node[A],
                                               @volatile private var _last: Node[A],
                                               @volatile private var _size: Int) { self =>

  def size: Int =
    _size

  def isEmpty =
    _size == 0

  def nonEmpty =
    !isEmpty

  def addHead(value: A): VolatileQueue[A] =
    self.synchronized {

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
    self.synchronized {
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
    self.synchronized {
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
            _head = Node.Empty
            _last = Node.Empty

          case previous: Node.Value[A] =>
            //unlink
            _size -= 1
            previous.next = Node.Empty
            _last = previous
        }
    }

  def replaceLast(expectedLast: A, replaceWith: A): Unit =
    self.synchronized {
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
            _head = newLast
            _last = newLast

          case previous: Node.Value[A] =>
            val newLast = new Node.Value[A](replaceWith, previous, Node.Empty)
            previous.next = newLast
            _last = newLast
        }
    }

  def replaceLastTwo(expectedSecondLast: A, expectedLast: A, replaceWith: A): Unit =
    self.synchronized {
      if (_last.isEmpty)
        throw new Exception("Last is empty")
      else if (_last.value != expectedLast)
        throw new Exception(s"Invalid remove. ${_last.value} != $expectedLast")
      else if (_last.previous.value != expectedSecondLast)
        throw new Exception(s"Invalid remove. ${_last.previous} != $expectedSecondLast")
      else
        _last.previous match {
          case Node.Empty =>
            throw new Exception("SecondLast is empty")

          case previous: Node.Value[A] =>
            previous.previous match {
              case Node.Empty =>
                assert(size == 2)
                val newLast = new Node.Value[A](replaceWith, Node.Empty, Node.Empty)
                _head = newLast
                _last = newLast

                _size -= 1

              case previousPrevious: Node.Value[A] =>
                val newLast = new Node.Value[A](replaceWith, previousPrevious, Node.Empty)
                previousPrevious.next = newLast
                _last = newLast

                _size -= 1
            }
        }
    }

  def head(): Option[A] =
    Option(headOrNull())

  def headOrNull(): A = {
    //read the value first
    val head = _head
    if (head.isEmpty)
      null
    else
      head.value
  }

  def last(): Option[A] =
    Option(lastOrNull())

  def lastOrNull(): A = {
    //read the value first
    val last = _last
    if (last.isEmpty)
      null
    else
      last.value
  }

  def secondLast(): Option[A] =
    Option(secondLastOrNull())

  def secondLastOrNull(): A = {
    //read the value first
    val secondLast = _last.previous
    if (secondLast.isEmpty)
      null
    else
      secondLast.value
  }

  def lastTwo(): Option[(A, A)] =
    Option(lastTwoOrNull())

  def lastTwoOrNull(): (A, A) = {
    //read the value first
    val last = _last
    val secondLast = last.previous
    if (last.isEmpty || secondLast.isEmpty)
      null
    else
      (secondLast.value, last.value)
  }

  def iterator: Iterator[A] =
    new Iterator[A] {
      var node: Node[A] = self._head
      var value: A = _

      //TODO - handle if hasNext is called multiple times.
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

  def dropIterator: DropIterator.Flat[Null, A] =
    DropIterator(self.iterator)
}
