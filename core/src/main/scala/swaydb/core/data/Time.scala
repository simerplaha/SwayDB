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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.data

import swaydb.IO
import swaydb.data.order.TimeOrder
import swaydb.data.slice.Slice
import swaydb.data.util.{ByteSizeOf, Bytez}

private[core] object Time {

  val empty = Time(Slice.emptyBytes)
  val someEmpty = Some(empty)
  val successEmpty = IO.Right[Nothing, Time](empty)(IO.ExceptionHandler.Nothing)

  def apply(time: Long): Time = {
    val slice = Slice.create[Byte](ByteSizeOf.varLong)
    Bytez.writeUnsignedLong(time, slice)
    new Time(slice)
  }

  def >(upperTime: Time, lowerTime: Time)(implicit timeOrder: TimeOrder[Slice[Byte]]): Boolean = {
    import timeOrder._
    if (upperTime.nonEmpty && lowerTime.nonEmpty)
      upperTime.time > lowerTime.time
    else
      true //if timeOne and timeTwo are the same this means the upper is already merged into lower so lower is greater.
  }

  implicit class TimeOptionImplicits(time: Time) {
    @inline final def >(otherTime: Time)(implicit timeOrder: TimeOrder[Slice[Byte]]): Boolean =
      Time > (time, otherTime)
  }

  def fromApplies(applies: Slice[Value.Apply]): Time =
    applies
      .reverse
      .find(_.time.nonEmpty)
      .map(_.time)
      .getOrElse(Time.empty)
}

private[core] case class Time private(time: Slice[Byte]) {
  def unslice(): Time =
    Time(time.unslice())

  def isEmpty =
    time.isEmpty

  def nonEmpty =
    !isEmpty

  def size =
    time.size
}
