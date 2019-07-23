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

package swaydb.core.data

import java.util.concurrent.atomic.AtomicLong

import swaydb.IO
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice

private[core] object Time {

  val empty = Time(Slice.emptyBytes)
  val someEmpty = Some(empty)
  val successEmpty = IO.Success(empty)

  val long = new AtomicLong(System.nanoTime())

  def localNano: Time =
    Time(Slice.writeLong(long.incrementAndGet()))

  def apply(time: Long): Time =
    new Time(Slice.writeLong(time))

  def >(upperTime: Time, lowerTime: Time)(implicit timeOrder: TimeOrder[Slice[Byte]]): Boolean = {
    import timeOrder._
    if (upperTime.nonEmpty && lowerTime.nonEmpty)
      upperTime.time > lowerTime.time
    else
      true //if timeOne and timeTwo are the same this means the upper is already merged into lower so lower is greater.
  }

  implicit class TimeOptionImplicits(time: Time) {
    def >(otherTime: Time)(implicit timeOrder: TimeOrder[Slice[Byte]]): Boolean =
      Time > (time, otherTime)
  }

  def fromApplies(applies: Slice[Value.Apply]): Time =
    applies
      .reverse
      .find(_.time.nonEmpty)
      .map(_.time)
      .getOrElse(Time.empty)
}

private[core] case class Time(time: Slice[Byte]) {
  def unslice(): Time =
    Time(time.unslice())

  def isEmpty =
    time.isEmpty

  def nonEmpty =
    !isEmpty

  def size =
    time.size
}
