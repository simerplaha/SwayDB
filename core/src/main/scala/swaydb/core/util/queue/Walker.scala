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

import scala.annotation.tailrec

private[core] trait Walker[A >: Null] { self =>
  def headOrNull(): A

  def head(): Node[A]

  def dropHead(): Walker[A] =
    head() match {
      case Node.Empty =>
        Walker.empty

      case head: Node.Value[A] =>
        head.next match {
          case Node.Empty =>
            new VolatileQueue[A](Node.Empty, Node.Empty)

          case next: Node.Value[A] =>
            new VolatileQueue[A](next, next.next)
        }
    }

  def flatMap[B >: Null](f: A => Walker[B]): Walker[B] =
    new Walker[B] {

      var aStepper: Walker[A] = self
      var bStepper: Walker[B] = _

      @tailrec
      def getHead(drop: Int): Node[B] =
        if (bStepper == null)
          aStepper.head() match {
            case Node.Empty =>
              Node.Empty

            case value: Node.Value[A] =>
              bStepper = f(value.value)
              bStepper.head() match {
                case Node.Empty =>
                  aStepper = aStepper.dropHead()
                  bStepper = null
                  getHead(drop)

                case value: Node.Value[B] =>
                  if (drop > 0) {
                    bStepper = bStepper.dropHead()
                    getHead(drop - 1)
                  } else {
                    value
                  }
              }
          }
        else
          bStepper.head() match {
            case Node.Empty =>
              aStepper = aStepper.dropHead()
              bStepper = null
              getHead(drop)

            case value: Node.Value[B] =>
              if (drop > 0) {
                bStepper = bStepper.dropHead()
                getHead(drop - 1)
              } else {
                value
              }
          }

      override def headOrNull(): B =
        getHead(0) match {
          case Node.Empty =>
            null

          case value: Node.Value[B] =>
            value.value
        }

      override def head(): Node[B] =
        getHead(0)

      override def dropHead(): Walker[B] = {
        getHead(1)
        this
      }
    }

  def foreach(f: A => Unit): Unit = {
    var walker: Walker[A] = self
    var head = walker.headOrNull()
    while (head != null) {
      f(head)
      walker = walker.dropHead()
      head = walker.headOrNull()
    }
  }

  def findOrNull(f: A => Boolean): A = {
    var walker: Walker[A] = self
    var head = walker.headOrNull()

    while (head != null) {
      if (f(head))
        return head

      walker = walker.dropHead()
      head = walker.headOrNull()
    }

    null
  }
}

object Walker {
  def empty[A >: Null]: Walker[A] =
    new Walker[A] {
      override def headOrNull(): A = null
      override def head(): Node[A] = Node.Empty
    }
}
