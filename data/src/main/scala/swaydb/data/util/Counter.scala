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

package swaydb.data.util

private[swaydb] sealed trait Counter[T] {
  def incrementAndGet(): T

  def decrementAndGet(): T

  def get(): T
}

private[swaydb] object Counter {

  def forInt(start: Int): IntCounter =
    new IntCounter(start)

  class IntCounter(private var value: Int) extends Counter[Int] {
    override def incrementAndGet(): Int = {
      value += 1
      value
    }

    override def decrementAndGet(): Int = {
      value -= 1
      value
    }

    override def get(): Int =
      value
  }

  /**
   * Maintains a count of number of requests for an item.
   */
  object Request {
    def apply[T](item: T, start: Int): Request[T] =
      new Request(item, Counter.forInt(start))
  }

  class Request[+T](val item: T, val counter: Counter.IntCounter)
}
