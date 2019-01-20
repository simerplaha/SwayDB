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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.data

import scala.concurrent.duration.Deadline
import swaydb.data.slice.Slice

sealed trait SwayFunction

object SwayFunction {
  sealed trait RequiresKey extends SwayFunction
  sealed trait RequiresValue extends SwayFunction
  sealed trait RequiresDeadline extends SwayFunction

  case class Key(f: Slice[Byte] => SwayFunctionOutput) extends RequiresKey
  case class KeyDeadline(f: (Slice[Byte], Option[Deadline]) => SwayFunctionOutput) extends RequiresKey with RequiresDeadline
  case class KeyValue(f: (Slice[Byte], Option[Slice[Byte]]) => SwayFunctionOutput) extends RequiresKey with RequiresValue

  case class KeyValueDeadline(f: (Slice[Byte], Option[Slice[Byte]], Option[Deadline]) => SwayFunctionOutput) extends RequiresKey with RequiresValue  with RequiresDeadline
  case class Value(f: Option[Slice[Byte]] => SwayFunctionOutput) extends RequiresValue
  case class ValueDeadline(f: (Option[Slice[Byte]], Option[Deadline]) => SwayFunctionOutput) extends RequiresValue  with RequiresDeadline
}

sealed trait SwayFunctionOutput {
  def toValue(time: Time): Value.RangeValue
}
object SwayFunctionOutput {

  case object Remove extends SwayFunctionOutput {
    def toValue(time: Time): Value.Remove =
      Value.Remove(None, time)
  }

  case class Expire(deadline: Deadline) extends SwayFunctionOutput {
    def toValue(time: Time): Value.Remove =
      Value.Remove(Some(deadline), time)
  }

  case class Update(value: Option[Slice[Byte]], deadline: Option[Deadline]) extends SwayFunctionOutput {
    def toValue(time: Time): Value.Update =
      Value.Update(value, deadline, time)
  }
}
