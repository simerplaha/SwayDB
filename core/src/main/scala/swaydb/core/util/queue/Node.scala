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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util.queue

private[queue] sealed trait Node[+A] {
  def isEmpty: Boolean
  def previous: Node[A]
  def next: Node[A]
  def value: A
}

private[queue] case object Node {

  final case object Empty extends Node[Nothing] {
    override val isEmpty: Boolean = true
    override val previous: Node[Nothing] = Node.Empty
    override val next: Node[Nothing] = Node.Empty
    override def value: Nothing = throw new Exception(s"${Node.productPrefix}.${Empty.productPrefix} does not have a value.")
  }

  class Value[A](val value: A,
                 @volatile var previous: Node[A],
                 @volatile var next: Node[A]) extends Node[A] {
    override def isEmpty: Boolean = false
  }

}