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

package swaydb.core.data

import swaydb.IO
import swaydb.data.order.TimeOrder
import swaydb.slice.Slice
import swaydb.slice.utils.ScalaByteOps
import swaydb.utils.ByteSizeOf

private[core] object Time {

  val empty = Time(Slice.emptyBytes)
  val someEmpty = Some(empty)
  val successEmpty = IO.Right[Nothing, Time](empty)(IO.ExceptionHandler.Nothing)

  def apply(time: Long): Time = {
    val slice = Slice.of[Byte](ByteSizeOf.varLong)
    ScalaByteOps.writeUnsignedLong(time, slice)
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
  def cut(): Time =
    Time(time.cut())

  def isEmpty =
    time.isEmpty

  def nonEmpty =
    !isEmpty

  def size =
    time.size
}
